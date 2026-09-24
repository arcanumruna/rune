// ClientGatewayEgressTest.cpp — Unit tests for the G0.1 egress retry queue.
//
// Tests the invariant: a produce ack that encounters back-pressure on its
// first offer attempt is never silently dropped — it is queued and retried
// by drainPendingResponses() until the publication clears.
//
// Design:
//   - Uses the test-only makeForTest() factory to construct a ClientGateway
//     without a live Aeron media driver.
//   - Injects a FakePublication (a simple struct — NOT a subclass of
//     aeron::ExclusivePublication) via the TestEgressFn callback installed
//     by makeForTest().
//   - No Aeron headers needed; no IPC transport; no busy-spin.
//
// Covered cases:
//   1. BackpressuredAckIsRetriedNotDropped — primary correctness assertion:
//      after N back-pressured calls, draining delivers the frame.
//   2. ImmediateDeliveryNoQueue — when offer succeeds on first attempt, the
//      ring stays empty.
//   3. MultipleBackpressuredAcks — two acks back-pressured, drained FIFO.
//   4. DrainStopsOnBackpressure — drain stops at the first still-pressured
//      frame (preserves FIFO order for subsequent cycles).

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
#include <optional>
#include <string>
#include <unistd.h>
#include <vector>

namespace rune::gateway {
namespace {

namespace pc = rune::protocol::client;

// ---------------------------------------------------------------------------
// Decoded response captured by FakePublication.
// ---------------------------------------------------------------------------
struct CapturedResponse {
    int64_t baseOffset    = 0;
    int64_t correlationId = 0;
    int32_t partition     = 0;
    int16_t errorCode     = 0;
};

// ---------------------------------------------------------------------------
// FakePublication — controls offer() return values and captures deliveries.
// ---------------------------------------------------------------------------
class FakePublication {
public:
    // Configure the next N offer() calls to return BACK_PRESSURED.
    void backpressureNext(int n) { backpressureRemaining_ = n; }

    // Number of frames that were successfully delivered.
    int deliveredCount() const { return static_cast<int>(delivered_.size()); }

    // The most recently delivered response (decoded from raw bytes).
    const CapturedResponse& lastResponse() const { return delivered_.back(); }

    // Build a TestEgressFn that delegates to this FakePublication.
    // The returned function borrows `this` — the FakePublication must outlive the gateway.
    ClientGateway::TestEgressFn makeSendFn() {
        return [this](const uint8_t* buf, std::size_t len) -> int64_t {
            if (backpressureRemaining_ > 0) {
                --backpressureRemaining_;
                // Return BACK_PRESSURED sentinel (Aeron defines as -2).
                return -2;
            }
            // Deliver: decode the ProduceResponse and record it.
            auto decoded = pc::ProduceResponse::decode(buf, len);
            if (decoded) {
                CapturedResponse cr;
                cr.baseOffset    = decoded->baseOffset;
                cr.correlationId = decoded->correlationId;
                cr.partition     = decoded->partition;
                cr.errorCode     = decoded->errorCode;
                delivered_.push_back(cr);
            }
            return 1;  // > 0 = success
        };
    }

private:
    int                          backpressureRemaining_ = 0;
    std::vector<CapturedResponse> delivered_;
};

// ---------------------------------------------------------------------------
// Convenience: monotonic nanoseconds.
// ---------------------------------------------------------------------------
static int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// makeGatewayWith — construct a test gateway wired to the given FakePublication.
// currentSessionId is the session id that sendRespReliable() targets.
// ---------------------------------------------------------------------------
static ClientGateway makeGatewayWith(FakePublication& pub, int64_t sessionId = 1)
{
    return ClientGateway::makeForTest(pub.makeSendFn(), sessionId);
}

// ===========================================================================
// TEST 1 — BackpressuredAckIsRetriedNotDropped (primary correctness assertion)
//
// A produce ack that is back-pressured on its first offer attempt must
// eventually be delivered by drainPendingResponses() — never dropped silently.
// ===========================================================================
TEST(ClientGatewayEgress, BackpressuredAckIsRetriedNotDropped)
{
    FakePublication pub;
    pub.backpressureNext(3);  // first 3 offer() calls return BACK_PRESSURED
    ClientGateway gw = makeGatewayWith(pub);

    // The ack is accepted (queued) even though the offer is back-pressured.
    ASSERT_TRUE(gw.sendRespReliable(pc::ErrorCode::OK, /*partition*/0,
                                    /*baseOffset*/42, /*correlationId*/7));

    // Not delivered yet — all 3 back-pressure events precede any real delivery.
    EXPECT_EQ(pub.deliveredCount(), 0);
    EXPECT_EQ(gw.pendingResponseCount(), 1u);

    // Drain 5 times; after the 3rd drain the publication accepts the frame.
    for (int i = 0; i < 5; ++i) {
        gw.drainPendingResponses(nowNs());
    }

    // Exactly one frame was delivered.
    ASSERT_EQ(pub.deliveredCount(), 1);
    EXPECT_EQ(pub.lastResponse().baseOffset,    42);
    EXPECT_EQ(pub.lastResponse().correlationId,  7);
    EXPECT_EQ(pub.lastResponse().partition,       0);

    // Ring is now empty.
    EXPECT_EQ(gw.pendingResponseCount(), 0u);
}

// ===========================================================================
// TEST 2 — ImmediateDeliveryNoQueue
//
// When offer() succeeds on the first attempt, nothing should be enqueued.
// ===========================================================================
TEST(ClientGatewayEgress, ImmediateDeliveryNoQueue)
{
    FakePublication pub;
    // No back-pressure — offer() succeeds immediately.
    ClientGateway gw = makeGatewayWith(pub);

    ASSERT_TRUE(gw.sendRespReliable(pc::ErrorCode::OK, 0, 100, 42));

    EXPECT_EQ(pub.deliveredCount(), 1);
    EXPECT_EQ(gw.pendingResponseCount(), 0u);
    EXPECT_EQ(pub.lastResponse().baseOffset,    100);
    EXPECT_EQ(pub.lastResponse().correlationId,  42);
}

// ===========================================================================
// TEST 3 — MultipleBackpressuredAcksDrainFIFO
//
// Two acks are back-pressured; when back-pressure clears, both are drained
// in FIFO (first-in, first-out) order.
// ===========================================================================
TEST(ClientGatewayEgress, MultipleBackpressuredAcksDrainFIFO)
{
    FakePublication pub;
    pub.backpressureNext(2);  // first 2 offers are back-pressured
    ClientGateway gw = makeGatewayWith(pub);

    // Send two acks while back-pressured.
    ASSERT_TRUE(gw.sendRespReliable(pc::ErrorCode::OK, 0, 10, 100));
    ASSERT_TRUE(gw.sendRespReliable(pc::ErrorCode::OK, 1, 20, 200));

    EXPECT_EQ(pub.deliveredCount(), 0);
    EXPECT_EQ(gw.pendingResponseCount(), 2u);

    // After draining, both should be delivered in order.
    gw.drainPendingResponses(nowNs());
    gw.drainPendingResponses(nowNs());
    gw.drainPendingResponses(nowNs());

    ASSERT_EQ(pub.deliveredCount(), 2);
    // FIFO: first enqueued (correlationId=100) delivered first.
    EXPECT_EQ(pub.lastResponse().baseOffset,    20);
    EXPECT_EQ(pub.lastResponse().correlationId, 200);
    EXPECT_EQ(gw.pendingResponseCount(), 0u);
}

// ===========================================================================
// TEST 4 — DrainStopsOnBackpressure
//
// If back-pressure is still present on the first queued frame, drain stops
// immediately (preserves FIFO; later frames are not lost).
// ===========================================================================
TEST(ClientGatewayEgress, DrainStopsOnBackpressure)
{
    FakePublication pub;
    pub.backpressureNext(1);  // first offer will be back-pressured
    ClientGateway gw = makeGatewayWith(pub);

    ASSERT_TRUE(gw.sendRespReliable(pc::ErrorCode::OK, 0, 99, 55));
    EXPECT_EQ(gw.pendingResponseCount(), 1u);

    // First drain attempt: the pub is still back-pressured.
    pub.backpressureNext(1);  // next offer (the drain retry) is also pressured
    int delivered = gw.drainPendingResponses(nowNs());
    EXPECT_EQ(delivered, 0);
    EXPECT_EQ(gw.pendingResponseCount(), 1u);   // still in queue

    // Second drain attempt: now the publication accepts.
    delivered = gw.drainPendingResponses(nowNs());
    EXPECT_EQ(delivered, 1);
    EXPECT_EQ(gw.pendingResponseCount(), 0u);
    ASSERT_EQ(pub.deliveredCount(), 1);
    EXPECT_EQ(pub.lastResponse().baseOffset, 99);
}

// ===========================================================================
// PRODUCTION-PATH TEST — proves the actual handleProduce ack path is wired to
// the retry queue (not just the standalone sendRespReliable test-seam).
//
// This is the review-mandated guard: drive a real Produce frame through
// handleProduce() (inline path, no WorkerPool) with a back-pressured egress and
// assert the OK ack is enqueued (not dropped) and later delivered by
// drainPendingResponses().  If handleProduce's sendResp lambda ever stops
// routing through enqueueOrBestEffort_, this test fails.
// ===========================================================================

namespace fs = std::filesystem;
using rune::node::TopicSpec;

// Unique temp dir (getpid + atomic counter).
static fs::path makeTempDir()
{
    static std::atomic<uint32_t> seq{0};
    auto name = std::string("rune_gw_egress_") +
                std::to_string(static_cast<long>(::getpid())) + "_" +
                std::to_string(seq.fetch_add(1, std::memory_order_relaxed));
    return fs::temp_directory_path() / name;
}

// Build a single-record RecordBatch (baseOffset placeholder; gateway restamps).
static std::vector<uint8_t> makeBatch()
{
    rune::protocol::RecordBatchBuilder b(/*baseOffset=*/0, /*baseTimestamp=*/1000LL);
    b.append("k", "v", 1000LL);
    return b.build();
}

TEST(ClientGatewayEgress, HandleProduceAckIsRetriedNotDropped)
{
    // Back-pressure the FIRST egress offer so the OK ack cannot be delivered
    // synchronously inside handleProduce.
    FakePublication pub;
    pub.backpressureNext(1);

    // TopicRegistry with one topic (id=0, 1 partition) backed by a temp dir.
    const fs::path dir = makeTempDir();
    std::vector<TopicSpec> specs;
    {
        TopicSpec ts;
        ts.name           = "events";
        ts.partitionCount = 1;
        ts.topicId        = 0;
        specs.push_back(ts);
    }
    TopicRegistry registry(specs, dir.string());

    // Gateway wired with the fake egress + topic registry.
    ClientGateway gw = ClientGateway::makeForTest(pub.makeSendFn(), /*sessionId=*/1);
    gw.setTopicRegistry(&registry);

    // Inject a session with id=1 (matches makeForTest currentProduceSessionId).
    const int64_t sid = gw.injectTestSession(/*nowNs=*/0);
    ASSERT_EQ(sid, 1) << "injected session id must match currentProduceSessionId_";

    // Build + encode a Produce frame for topic 0 / partition 0, acks=Leader.
    auto batch = makeBatch();
    pc::Produce prod;
    prod.sessionId      = sid;
    prod.correlationId  = 777;
    prod.topicId        = 0;
    prod.partition      = 0;
    prod.partitionKnown = 1;
    prod.acks           = 0;   // acks=Leader → immediate ack after append
    prod.producerId     = -1;  // legacy (no idempotent dedup)
    prod.key            = {};
    prod.recordBatch    = std::span<const uint8_t>(batch.data(), batch.size());

    std::vector<uint8_t> frame(64 * 1024, 0);
    const std::size_t frameLen = prod.encode(frame.data());

    // Drive the real production handleProduce() path.
    gw.applyProduceForTest(frame.data(), frameLen);

    // The OK ack hit back-pressure and MUST have been enqueued (not dropped).
    EXPECT_EQ(pub.deliveredCount(), 0)
        << "ack was delivered despite back-pressure — unexpected";
    ASSERT_EQ(gw.pendingResponseCount(), 1u)
        << "handleProduce ack was NOT routed through the retry queue "
           "(silent-drop regression on the production path)";

    // Drain: the ack is now delivered with the correct fields.
    gw.drainPendingResponses(nowNs());
    ASSERT_EQ(pub.deliveredCount(), 1);
    EXPECT_EQ(pub.lastResponse().correlationId, 777);
    EXPECT_EQ(pub.lastResponse().errorCode,
              static_cast<int16_t>(pc::ErrorCode::OK));
    EXPECT_EQ(pub.lastResponse().baseOffset, 0);  // first append → offset 0
    EXPECT_EQ(gw.pendingResponseCount(), 0u);

    // Cleanup temp dir.
    std::error_code ec;
    fs::remove_all(dir, ec);
}

} // namespace
} // namespace rune::gateway
