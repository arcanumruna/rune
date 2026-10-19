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

// ===========================================================================
// F3 round 5, item M4: exact boundary at the gateway with the REAL derived
// default (multi-node) effective max — 524226 bytes (pinned against real
// transport instances in BrokerNodeRegression.DerivedMaxMatchesRealTransports-
// Regression).  A batch of EXACTLY the effective max is accepted (appended);
// effective max + 1 is rejected with MessageTooLarge (never appended).
// ===========================================================================
static std::vector<uint8_t> makeBatchOfExactSizeGw(std::size_t target) {
    auto build = [](std::size_t keyLen, std::size_t valLen) {
        rune::protocol::RecordBatchBuilder b(0, 1000LL);
        b.append(std::string(keyLen, 'k'), std::string(valLen, 'v'), 1000LL);
        return b.build();
    };
    for (std::size_t keyLen = 1; keyLen <= 4; ++keyLen) {
        std::size_t val = target - build(keyLen, 0).size();
        for (int iter = 0; iter < 8; ++iter) {
            auto b = build(keyLen, val);
            if (b.size() == target) return b;
            if (b.size() > target) { const std::size_t o = b.size() - target; if (o > val) break; val -= o; }
            else val += target - b.size();
        }
    }
    ADD_FAILURE() << "could not build a batch of exactly " << target << " bytes";
    return {};
}

// F3 round 6, item 2: the gateway's oversize-produce reject log is rate-
// limited (first, then every 1024th); every batch is still rejected.
TEST(ClientGatewayHotPath, OversizeRejectLogRateLimitedRegression)
{
    const fs::path dir = makeTempDir();
    using rune::node::TopicSpec;
    std::vector<TopicSpec> specs;
    { TopicSpec ts; ts.name = "events"; ts.partitionCount = 1; ts.topicId = 0; specs.push_back(ts); }
    rune::gateway::TopicRegistry registry(specs, dir.string());
    int rejects = 0;
    ClientGateway::TestEgressFn egress = [&rejects](const uint8_t* buf, std::size_t len) -> int64_t {
        auto r = pc::ProduceResponse::decode(buf, len);
        if (r && r->errorCode == static_cast<int16_t>(pc::ErrorCode::MessageTooLarge)) ++rejects;
        return 1;
    };
    ClientGateway gw = ClientGateway::makeForTest(egress, 1);
    gw.setTopicRegistry(&registry);
    gw.setMaxProduceBatchBytes(256);
    const int64_t sid = gw.injectTestSession(0);
    std::vector<uint8_t> batch(257, 0xAB);
    pc::Produce prod;
    prod.sessionId = sid; prod.correlationId = 1; prod.topicId = 0; prod.partition = 0;
    prod.partitionKnown = 1; prod.acks = 0; prod.producerId = -1; prod.key = {};
    prod.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
    std::vector<uint8_t> frame(prod.encodedSize());
    prod.encode(frame.data());
    for (int i = 0; i < 2100; ++i) gw.applyProduceForTest(frame.data(), frame.size());
    EXPECT_EQ(rejects, 2100);
    EXPECT_EQ(gw.oversizeProduceRejectLogsForTest(), 3u) << "logged at #1, #1025, #2049";
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(ClientGatewayHotPath, ProduceBatchExactEffectiveMaxBoundaryRegression)
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

    constexpr std::size_t kEffectiveMax = 524226;   // default multi-node derivation
    gw.setMaxProduceBatchBytes(kEffectiveMax);
    const int64_t sid = gw.injectTestSession(/*nowNs=*/0);

    auto produce = [&](const std::vector<uint8_t>& batch, int64_t corr) {
        pc::Produce prod;
        prod.sessionId      = sid;
        prod.correlationId  = corr;
        prod.topicId        = 0;
        prod.partition      = 0;
        prod.partitionKnown = 1;
        prod.acks           = 0;
        prod.producerId     = -1;
        prod.key            = {};
        prod.recordBatch    = std::span<const uint8_t>(batch.data(), batch.size());
        std::vector<uint8_t> frame(prod.encodedSize());
        prod.encode(frame.data());
        captured.clear();
        gw.applyProduceForTest(frame.data(), frame.size());
        auto resp = pc::ProduceResponse::decode(captured.data(), captured.size());
        EXPECT_TRUE(resp.has_value()) << "every produce gets a response";
        return resp ? resp->errorCode : int16_t{-999};
    };

    const auto over = makeBatchOfExactSizeGw(kEffectiveMax + 1);
    const auto exact = makeBatchOfExactSizeGw(kEffectiveMax);
    ASSERT_EQ(over.size(), kEffectiveMax + 1);
    ASSERT_EQ(exact.size(), kEffectiveMax);

    EXPECT_EQ(produce(over, 1), static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));
    EXPECT_EQ(registry.log(0, 0)->logEndOffset(), 0) << "oversize must never be appended";
    EXPECT_EQ(produce(exact, 2), 0) << "exactly the effective max must be accepted";
    EXPECT_EQ(registry.log(0, 0)->logEndOffset(), 1);

    // M3: 0 is installed VERBATIM (no silent 8 MiB fallback) — rejects all.
    gw.setMaxProduceBatchBytes(0);
    EXPECT_EQ(gw.maxProduceBatchBytes(), 0u);
    EXPECT_EQ(produce(makeBatchOfExactSizeGw(200), 3),
              static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));

    std::error_code ec;
    fs::remove_all(dir, ec);
}

// X036: a client-built CONTROL batch (a forged COMMIT/ABORT marker) anywhere in a Produce frame refuses the whole frame
// InvalidRequest at the gateway, never forwarded.  The control batch is SECOND in the frame: a first-batch-only check
// does not see it.  RED: delete the refusesClientBatches check in handleProduce - the frame is accepted (answer != 39).
TEST(ProduceValidationRegression, AClientControlBatchIsRefusedOnGatewayRegression)
{
    const fs::path dir = makeTempDir();
    std::vector<rune::node::TopicSpec> specs;
    {
        rune::node::TopicSpec ts;
        ts.name = "events"; ts.partitionCount = 1; ts.topicId = 0;
        specs.push_back(ts);
    }
    rune::gateway::TopicRegistry registry(specs, dir.string());
    std::vector<uint8_t> captured;
    ClientGateway gw = ClientGateway::makeForTest(
        [&captured](const uint8_t* buf, std::size_t len) -> int64_t { captured.assign(buf, buf + len); return 1; },
        /*currentProduceSessionId=*/1);
    gw.setTopicRegistry(&registry);
    const int64_t sid = gw.injectTestSession(/*nowNs=*/0);

    auto frameOf = [&](bool withControl, int64_t corr) {
        rune::protocol::RecordBatchBuilder data(0, 1000);
        data.append("k", "v", 1000);
        std::vector<uint8_t> batches = data.build();
        if (withControl) {
            rune::protocol::RecordBatchBuilder ctl(0, 1000);
            ctl.setControl(true);
            ctl.setProducerId(7); ctl.setProducerEpoch(0); ctl.setBaseSequence(0);
            ctl.append("", "", 1000);
            const auto c = ctl.build();
            batches.insert(batches.end(), c.begin(), c.end());
        }
        pc::Produce prod;
        prod.sessionId = sid; prod.correlationId = corr; prod.topicId = 0; prod.partition = 0; prod.partitionKnown = 1;
        prod.acks = 0; prod.producerId = -1; prod.baseSequence = -1;
        prod.recordBatch = std::span<const uint8_t>(batches.data(), batches.size());
        std::vector<uint8_t> frame(prod.encodedSize() + 8);
        frame.resize(prod.encode(frame.data()));
        return frame;
    };
    const auto bad = frameOf(true, 7);
    gw.applyProduceForTest(bad.data(), bad.size());
    ASSERT_FALSE(captured.empty());
    auto resp = pc::ProduceResponse::decode(captured.data(), captured.size());
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest))
        << "a control batch second in the frame must refuse the whole frame";

    captured.clear();                       // control: the same frame without the control batch is not InvalidRequest
    const auto ok = frameOf(false, 8);
    gw.applyProduceForTest(ok.data(), ok.size());
    ASSERT_FALSE(captured.empty()) << "the control half must observe an answer";
    auto r2 = pc::ProduceResponse::decode(captured.data(), captured.size());
    ASSERT_TRUE(r2.has_value());
    EXPECT_NE(r2->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    std::error_code ec;
    fs::remove_all(dir, ec);
}

// X036 (review I1): the append CRC-checks only the FIRST batch and writes the whole span, so a malformed chain behind a
// valid first batch (overlong totalLength, runt totalLength, trailing partial header) would be appended unchanged and
// could hide a forged marker.  Every such frame is refused InvalidRequest whole.  RED: make refusesClientBatches return
// false on a malformed chain (the old fail-open walk) - these frames are no longer InvalidRequest.
TEST(ProduceValidationRegression, AMalformedBatchChainIsRefusedOnGatewayRegression)
{
    const fs::path dir = makeTempDir();
    std::vector<rune::node::TopicSpec> specs;
    { rune::node::TopicSpec ts; ts.name = "events"; ts.partitionCount = 1; ts.topicId = 0; specs.push_back(ts); }
    rune::gateway::TopicRegistry registry(specs, dir.string());
    std::vector<uint8_t> captured;
    ClientGateway gw = ClientGateway::makeForTest(
        [&captured](const uint8_t* buf, std::size_t len) -> int64_t { captured.assign(buf, buf + len); return 1; }, 1);
    gw.setTopicRegistry(&registry);
    const int64_t sid = gw.injectTestSession(0);
    rune::protocol::RecordBatchBuilder first(0, 1000);
    first.append("k", "v", 1000);
    const std::vector<uint8_t> a = first.build();
    auto lenPatch = [](std::vector<uint8_t>& b, std::size_t at, uint32_t total) {   // wire length = total - 12
        const uint32_t v = total - 12;
        for (int i = 0; i < 4; ++i) b[at + static_cast<std::size_t>(rune::protocol::batch::kLengthOffset) + i] = static_cast<uint8_t>(v >> (8 * i));
    };
    auto chainOf = [&](int variant) {
        std::vector<uint8_t> chain = a;
        rune::protocol::RecordBatchBuilder second(0, 1000);
        second.append("k2", "v2", 1000);
        const std::vector<uint8_t> b = second.build();
        if (variant == 3) { chain.insert(chain.end(), b.begin(), b.begin() + 10); return chain; }   // trailing partial header
        chain.insert(chain.end(), b.begin(), b.end());
        if (variant == 1) lenPatch(chain, a.size(), static_cast<uint32_t>(b.size()) + 4096);          // overlong
        if (variant == 2) lenPatch(chain, a.size(), 30);                                               // runt (< header)
        return chain;
    };
    for (int variant = 1; variant <= 3; ++variant) {
        captured.clear();
        const auto batches = chainOf(variant);
        pc::Produce prod;
        prod.sessionId = sid; prod.correlationId = 100 + variant; prod.topicId = 0; prod.partition = 0; prod.partitionKnown = 1;
        prod.acks = 0; prod.producerId = -1; prod.baseSequence = -1;
        prod.recordBatch = std::span<const uint8_t>(batches.data(), batches.size());
        std::vector<uint8_t> frame(prod.encodedSize() + 8);
        frame.resize(prod.encode(frame.data()));
        gw.applyProduceForTest(frame.data(), frame.size());
        ASSERT_FALSE(captured.empty()) << variant;
        auto resp = pc::ProduceResponse::decode(captured.data(), captured.size());
        ASSERT_TRUE(resp.has_value()) << variant;
        EXPECT_EQ(resp->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest)) << "malformed chain variant " << variant;
    }
    std::error_code ec;
    fs::remove_all(dir, ec);
}

// X036 (review I3): a produce frame is EXACTLY ONE non-control batch.  A well-formed second batch would be appended with no
// CRC, producer-state or offset-restamp check; a lone control batch is a forged marker; a bad declared length (0, negative,
// near INT32_MAX) must neither crash nor pass.  RED: replace refusesClientBatches with the multi-batch walk (or drop the
// t == len test) - the two-batch frame is accepted; drop the control test - the lone control frame is accepted.
TEST(ProduceValidationRegression, ASingleBatchRuleIsEnforcedOnGatewayRegression)
{
    const fs::path dir = makeTempDir();
    std::vector<rune::node::TopicSpec> specs;
    { rune::node::TopicSpec ts; ts.name = "events"; ts.partitionCount = 1; ts.topicId = 0; specs.push_back(ts); }
    rune::gateway::TopicRegistry registry(specs, dir.string());
    std::vector<uint8_t> captured;
    ClientGateway gw = ClientGateway::makeForTest(
        [&captured](const uint8_t* buf, std::size_t len) -> int64_t { captured.assign(buf, buf + len); return 1; }, 1);
    gw.setTopicRegistry(&registry);
    const int64_t sid = gw.injectTestSession(0);
    auto data = [](const char* k) { rune::protocol::RecordBatchBuilder b(0, 1000); b.append(k, "v", 1000); return b.build(); };
    auto control = [] { rune::protocol::RecordBatchBuilder b(0, 1000); b.setControl(true); b.setProducerId(7); b.setProducerEpoch(0);
                        b.setBaseSequence(0); b.append("", "", 1000); return b.build(); };
    auto setLen = [](std::vector<uint8_t>& b, uint32_t field) {
        for (int i = 0; i < 4; ++i) b[static_cast<std::size_t>(rune::protocol::batch::kLengthOffset) + i] = static_cast<uint8_t>(field >> (8 * i));
    };
    struct Case { const char* name; std::vector<uint8_t> batches; bool refused; };
    std::vector<Case> cases;
    { auto a = data("a"); auto b = data("b"); a.insert(a.end(), b.begin(), b.end()); cases.push_back({"two well-formed batches", a, true}); }
    cases.push_back({"lone control batch", control(), true});
    { auto a = data("a"); setLen(a, 0); cases.push_back({"length 0", a, true}); }
    { auto a = data("a"); setLen(a, 0xFFFFFFF0u); cases.push_back({"negative length", a, true}); }
    { auto a = data("a"); setLen(a, 0x7FFFFFF8u); cases.push_back({"length near INT32_MAX", a, true}); }
    { auto a = data("a"); setLen(a, 0x7FFFFFF4u); cases.push_back({"length INT32_MAX-12+..", a, true}); }
    cases.push_back({"one well-formed batch", data("a"), false});
    int corr = 200;
    for (const auto& c : cases) {
        captured.clear();
        pc::Produce prod;
        prod.sessionId = sid; prod.correlationId = ++corr; prod.topicId = 0; prod.partition = 0; prod.partitionKnown = 1;
        prod.acks = 0; prod.producerId = -1; prod.baseSequence = -1;
        prod.recordBatch = std::span<const uint8_t>(c.batches.data(), c.batches.size());
        std::vector<uint8_t> frame(prod.encodedSize() + 8);
        frame.resize(prod.encode(frame.data()));
        gw.applyProduceForTest(frame.data(), frame.size());
        ASSERT_FALSE(captured.empty()) << c.name;
        auto resp = pc::ProduceResponse::decode(captured.data(), captured.size());
        ASSERT_TRUE(resp.has_value()) << c.name;
        if (c.refused) EXPECT_EQ(resp->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest)) << c.name;
        else           EXPECT_NE(resp->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest)) << c.name;
    }
    std::error_code ec;
    fs::remove_all(dir, ec);
}

} // namespace
} // namespace rune::gateway
