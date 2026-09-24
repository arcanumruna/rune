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
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <optional>
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

} // namespace
} // namespace rune::gateway
