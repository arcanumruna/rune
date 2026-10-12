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
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/RecordBatch.h>

#include <rune/gateway/SpillFifo.h>
#include <rune/consensus/PeerResolution.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/gateway/RequestRejection.h>
#include <rune/gateway/IGroupRequestHandler.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/client/GroupMessages.h>
#include <gtest/gtest.h>
#include <deque>
#include <random>

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
// routing through sendReliable_, this test fails.
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


// ===========================================================================
// No-hang wave (R2): a request whose MessageHeader decodes on a known session
// is ANSWERED with its own response type + an error code on the header's
// correlationId — never dropped while the client waits out its timeout.
// ===========================================================================

// A frame that is a valid MessageHeader (known template, current version) and
// nothing else: every body decoder rejects it as truncated.
static std::vector<uint8_t> headerOnlyFrame(pc::TemplateId tid, int64_t sid, int64_t corr,
                                            uint16_t blockLength = 64)
{
    pc::MessageHeader h;
    h.blockLength   = blockLength;
    h.templateId    = static_cast<uint16_t>(tid);
    h.sessionId     = sid;
    h.correlationId = corr;
    std::vector<uint8_t> f(pc::MessageHeader::ENCODED_SIZE);
    h.encode(f.data());
    return f;
}

struct CaptureAll {
    std::vector<std::vector<uint8_t>> frames;
    int backpressure = 0;
    std::vector<int64_t> blockedSids;   // fix round 3: per-session back-pressure (header sessionId)
    ClientGateway::TestEgressFn fn() {
        return [this](const uint8_t* b, std::size_t n) -> int64_t {
            if (backpressure > 0) { --backpressure; return -2; }
            if (const auto h = pc::MessageHeader::decode(b, n)) {
                for (const int64_t sid : blockedSids) if (sid == h->sessionId) return -2;
            }
            frames.emplace_back(b, b + n);
            return 1;
        };
    }
    template <class Msg>
    std::optional<Msg> last() const {
        for (auto it = frames.rbegin(); it != frames.rend(); ++it)
            if (auto m = Msg::decode(it->data(), it->size())) return m;
        return std::nullopt;
    }
};

TEST(ClientGatewayEgressRegression, MalformedProduceIsAnsweredInvalidRequestRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    const int64_t sid = gw.injectTestSession(0);
    const auto f = headerOnlyFrame(pc::TemplateId::Produce, sid, /*corr=*/9001, /*blockLength=*/24);
    ASSERT_FALSE(pc::Produce::decode(f.data(), f.size()).has_value());
    gw.applyProduceForTest(f.data(), f.size());
    const auto r = cap.last<pc::ProduceResponse>();
    ASSERT_TRUE(r.has_value()) << "an undecodable Produce must be answered, not dropped";
    EXPECT_EQ(r->correlationId, 9001);
    EXPECT_EQ(r->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_EQ(gw.rejectedRequestsForTest(), 1u);
    EXPECT_EQ(gw.unanswerableFramesForTest(), 0u);
}

// acks is a two-valued wire byte: 2 is rejected by decode and answered (it
// used to read as acks=Leader on the gateway and acks=All on a worker).
TEST(ClientGatewayEgressRegression, ProduceUnknownAcksByteIsAnsweredInvalidRequestRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    const int64_t sid = gw.injectTestSession(0);
    auto batch = makeBatch();
    pc::Produce prod;
    prod.sessionId = sid; prod.correlationId = 31; prod.topicId = 0; prod.partition = 0;
    prod.partitionKnown = 1; prod.acks = 1; prod.producerId = -1;
    prod.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
    std::vector<uint8_t> frame(64 * 1024, 0);
    const std::size_t n = prod.encode(frame.data());
    frame[pc::MessageHeader::ENCODED_SIZE + 9] = 2;   // the acks byte
    gw.applyProduceForTest(frame.data(), n);
    const auto r = cap.last<pc::ProduceResponse>();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->correlationId, 31);
    EXPECT_EQ(r->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
}

// The answer to a malformed Produce rides the reliable egress: back-pressured,
// it is queued and delivered by the retry drain (never best-effort dropped).
TEST(ClientGatewayEgressRegression, MalformedProduceAnswerIsQueuedUnderBackpressureRegression)
{
    CaptureAll cap;
    cap.backpressure = 1;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    const int64_t sid = gw.injectTestSession(0);
    const auto f = headerOnlyFrame(pc::TemplateId::Produce, sid, 77, 24);
    gw.applyProduceForTest(f.data(), f.size());
    EXPECT_TRUE(cap.frames.empty());
    ASSERT_EQ(gw.pendingResponseCount(), 1u);
    gw.drainPendingResponses(nowNs());
    const auto r = cap.last<pc::ProduceResponse>();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->correlationId, 77);
    EXPECT_EQ(r->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
}

// Only an unknown session (or an undecodable header) may go unanswered — it is
// counted, never answered on someone else's session.
TEST(ClientGatewayEgressRegression, MalformedProduceForUnknownSessionIsCountedNotAnsweredRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    (void)gw.injectTestSession(0);
    const auto f = headerOnlyFrame(pc::TemplateId::Produce, /*sid=*/987654, 5, 24);
    gw.applyProduceForTest(f.data(), f.size());
    EXPECT_TRUE(cap.frames.empty());
    EXPECT_EQ(gw.unanswerableFramesForTest(), 1u);
    const std::vector<uint8_t> tooShort(10, 0);
    gw.applyProduceForTest(tooShort.data(), tooShort.size());
    EXPECT_TRUE(cap.frames.empty());
    EXPECT_EQ(gw.unanswerableFramesForTest(), 2u);
}

// Through the real ingress dispatch: a Ping whose body does not decode is
// answered ErrorResponse{InvalidRequest} (the client completes its pending ping).
TEST(ClientGatewayEgressRegression, MalformedPingIsAnsweredErrorResponseRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    const int64_t sid = gw.injectReliableTestSessionForTest(0);
    const auto f = headerOnlyFrame(pc::TemplateId::Ping, sid, 4321, 0);
    ASSERT_FALSE(pc::Ping::decode(f.data(), f.size()).has_value());
    (void)gw.applyClientFrameForTest(f.data(), f.size());
    const auto e = cap.last<pc::ErrorResponse>();
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->correlationId, 4321);
    EXPECT_EQ(e->code, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
}

// A frame checkFrame rejects (a newer protocol version, an unknown template)
// on a known session is answered ErrorResponse{ProtocolVersion}, not dropped.
TEST(ClientGatewayEgressRegression, CheckFrameRejectIsAnsweredProtocolVersionRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    const int64_t sid = gw.injectReliableTestSessionForTest(0);

    pc::MessageHeader h;
    h.blockLength = 0; h.templateId = static_cast<uint16_t>(pc::TemplateId::Keepalive);   // no response type
    h.version = static_cast<uint16_t>(pc::kProtocolVersion + 1);
    h.sessionId = sid; h.correlationId = 55;
    std::vector<uint8_t> f(pc::MessageHeader::ENCODED_SIZE);
    h.encode(f.data());
    (void)gw.applyClientFrameForTest(f.data(), f.size());
    auto e = cap.last<pc::ErrorResponse>();
    ASSERT_TRUE(e.has_value()) << "newer-version frame dropped silently";
    EXPECT_EQ(e->correlationId, 55);
    EXPECT_EQ(e->code, static_cast<int16_t>(pc::ErrorCode::ProtocolVersion));

    h.version = pc::kProtocolVersion; h.templateId = 30; h.correlationId = 56;   // unassigned id
    h.encode(f.data());
    (void)gw.applyClientFrameForTest(f.data(), f.size());
    e = cap.last<pc::ErrorResponse>();
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->correlationId, 56);
    EXPECT_EQ(e->code, static_cast<int16_t>(pc::ErrorCode::ProtocolVersion));

    // Fix round 2 (I6): a KNOWN request template with a newer version is
    // answered with ITS OWN response type (an ErrorResponse completes only
    // control ops on the client): a Produce → ProduceResponse{ProtocolVersion}.
    h.version = static_cast<uint16_t>(pc::kProtocolVersion + 1);
    h.templateId = static_cast<uint16_t>(pc::TemplateId::Produce); h.correlationId = 57;
    h.encode(f.data());
    (void)gw.applyClientFrameForTest(f.data(), f.size());
    const auto pr = cap.last<pc::ProduceResponse>();
    ASSERT_TRUE(pr.has_value()) << "a newer-version Produce was not answered with a ProduceResponse";
    EXPECT_EQ(pr->correlationId, 57);
    EXPECT_EQ(pr->errorCode, static_cast<int16_t>(pc::ErrorCode::ProtocolVersion));
    // ... and a JoinGroup → JoinGroupResponse{ProtocolVersion}.
    h.templateId = static_cast<uint16_t>(pc::TemplateId::JoinGroup); h.correlationId = 58;
    h.encode(f.data());
    (void)gw.applyClientFrameForTest(f.data(), f.size());
    const auto jr = cap.last<pc::JoinGroupResponse>();
    ASSERT_TRUE(jr.has_value());
    EXPECT_EQ(jr->correlationId, 58);
    EXPECT_EQ(jr->errorCode, static_cast<int16_t>(pc::ErrorCode::ProtocolVersion));
}

// Fix round 2 (I3): a txn handler returning 0 for anything but EndTxn (its only
// legal "deferred") is answered Internal, never read as "deferred".
// (CoordinatorRedirectTest's CountingTxn returns 0 for every request.)

// Fix round 2 (I4): a ViewRowResult whose response would exceed the egress
// frame is answered ViewRowResponse{RowTooLarge (7)} with no row bytes.
TEST(ClientGatewayEgressRegression, OversizedViewRowIsAnsweredRowTooLargeRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    const int64_t sid = gw.injectReliableTestSessionForTest(0);
    rune::node::WorkerReturn ret;
    ret.type          = rune::node::WorkerReturn::Type::ViewRowResult;
    ret.sessionId     = sid;
    ret.correlationId = 707;
    ret.errorCode     = 0;
    ret.chunk.assign(200 * 1024, 0x42);   // > the 68 KiB egress scratch
    ret.recordCount   = static_cast<int32_t>(ret.chunk.size());
    gw.handleWorkerReturnForTest(ret);
    const auto v = cap.last<pc::ViewRowResponse>();
    ASSERT_TRUE(v.has_value()) << "an oversized view row was dropped";
    EXPECT_EQ(v->correlationId, 707);
    EXPECT_EQ(v->status, pc::ViewRowResponse::kRowTooLarge);
    EXPECT_EQ(v->rowBytesLen, 0u);
}

// Fix round 2 (I5): a HandshakeContinue whose body does not decode fails the
// handshake in progress (HandshakeFailed on its correlationId, pending connect
// closed) instead of leaving the client waiting out its connect deadline.
TEST(ClientGatewayEgressRegression, UndecodableHandshakeContinueFailsTheHandshakeRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    gw.injectPendingHandshakeForTest(/*sessionId=*/4242);
    ASSERT_EQ(gw.pendingConnectCount(), 1u);
    const auto f = headerOnlyFrame(pc::TemplateId::HandshakeContinue, 4242, 515, 32);
    ASSERT_FALSE(pc::HandshakeContinue::decode(f.data(), f.size()).has_value());
    (void)gw.applyClientFrameForTest(f.data(), f.size());
    const auto r = cap.last<pc::HandshakeContinueResponse>();
    ASSERT_TRUE(r.has_value()) << "an undecodable HandshakeContinue left the handshake pending";
    EXPECT_EQ(r->correlationId, 515);
    EXPECT_EQ(r->code, static_cast<int16_t>(pc::ErrorCode::HandshakeFailed));
    EXPECT_EQ(gw.pendingConnectCount(), 0u) << "the failed handshake was not closed";
}

// Old clients (< kClientVersionInvalidRequest) re-join forever on InvalidRequest /
// Internal: their malformed JoinGroup is answered ProtocolVersion (non-retriable
// for them); a current client gets InvalidRequest.
TEST(ClientGatewayEgressRegression, OldClientMalformedJoinGroupIsAnsweredProtocolVersionRegression)
{
    struct NoGroup final : IGroupRequestHandler {
        size_t handleJoinGroup(const uint8_t* b, size_t n, int64_t, uint8_t* o, size_t c) override {
            return encodeRejectionFor(b, n, static_cast<uint16_t>(pc::ErrorCode::InvalidRequest), o, c);
        }
        size_t handleSyncGroup(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
        size_t handleHeartbeat(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
        size_t handleLeaveGroup(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
        size_t handleOffsetCommit(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
        size_t handleOffsetFetch(const uint8_t*, size_t, int64_t, uint8_t*, size_t) override { return 0; }
    } svc;
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    gw.setGroupService(&svc);
    const int64_t oldSid = gw.injectReliableTestSessionForTest(0, /*clientVersion=*/4);
    const int64_t newSid = gw.injectReliableTestSessionForTest(0, pc::kClientVersion);
    auto f = headerOnlyFrame(pc::TemplateId::JoinGroup, oldSid, 61, 32);
    gw.applyGroupForTest(f.data(), f.size(), pc::JoinGroup::TEMPLATE_ID);
    auto j = cap.last<pc::JoinGroupResponse>();
    ASSERT_TRUE(j.has_value());
    EXPECT_EQ(j->correlationId, 61);
    EXPECT_EQ(j->errorCode, static_cast<int16_t>(pc::ErrorCode::ProtocolVersion));
    f = headerOnlyFrame(pc::TemplateId::JoinGroup, newSid, 62, 32);
    gw.applyGroupForTest(f.data(), f.size(), pc::JoinGroup::TEMPLATE_ID);
    j = cap.last<pc::JoinGroupResponse>();
    ASSERT_TRUE(j.has_value());
    EXPECT_EQ(j->correlationId, 62);
    EXPECT_EQ(j->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
}

// sendControlFrame (DDL answers from the admin thread, deferred EndTxn, Assign)
// is queued under back-pressure — it used a best-effort send that lost it.
TEST(ClientGatewayEgressRegression, ControlFrameIsQueuedUnderBackpressureRegression)
{
    CaptureAll cap;
    cap.backpressure = 1;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    const int64_t sid = gw.injectReliableTestSessionForTest(0);
    pc::DefineTopicResponse resp;
    resp.sessionId = sid; resp.correlationId = 12; resp.topicId = 3; resp.version = 1;
    std::vector<uint8_t> f(resp.encodedSize());
    resp.encode(f.data());
    EXPECT_TRUE(gw.sendControlFrame(sid, f.data(), f.size()));
    EXPECT_TRUE(cap.frames.empty());
    ASSERT_EQ(gw.pendingResponseCount(), 1u) << "a back-pressured DDL answer was dropped";
    gw.drainPendingResponses(nowNs());
    const auto r = cap.last<pc::DefineTopicResponse>();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->correlationId, 12);
}


// Pong rides the reliable egress: back-pressured, it is queued + retried (it
// used a best-effort send — a lost Pong left the client's ping pending).
TEST(ClientGatewayEgressRegression, PongIsQueuedUnderBackpressureRegression)
{
    CaptureAll cap;
    cap.backpressure = 1;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    const int64_t sid = gw.injectReliableTestSessionForTest(0);
    pc::Ping ping;
    ping.sessionId = sid; ping.correlationId = 606; ping.echo = "hi";
    std::vector<uint8_t> f(pc::MessageHeader::ENCODED_SIZE + 2 + ping.echo.size());
    f.resize(ping.encode(f.data()));
    (void)gw.applyClientFrameForTest(f.data(), f.size());
    EXPECT_TRUE(cap.frames.empty());
    ASSERT_EQ(gw.pendingResponseCount(), 1u) << "a back-pressured Pong was dropped";
    gw.drainPendingResponses(nowNs());
    const auto pong = cap.last<pc::Pong>();
    ASSERT_TRUE(pong.has_value());
    EXPECT_EQ(pong->correlationId, 606);
}

// Requests with NO response type (Disconnect; a Keepalive has no body to fail)
// and pre-session legs (Connect, HandshakeContinue) cannot be answered: an
// undecodable one is counted + warned (rate-limited), never silently dropped
// and never answered with a foreign type.
TEST(ClientGatewayEgressRegression, UnanswerableMalformedFramesAreCountedRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    const int64_t sid = gw.injectReliableTestSessionForTest(0);
    uint64_t expect = 0;
    for (const auto tid : {pc::TemplateId::Disconnect,
                           pc::TemplateId::Connect, pc::TemplateId::HandshakeContinue}) {
        const auto f = headerOnlyFrame(tid, sid, 1, 64);
        (void)gw.applyClientFrameForTest(f.data(), f.size());
        EXPECT_EQ(gw.unanswerableFramesForTest(), ++expect) << "tid " << static_cast<int>(tid);
    }
    EXPECT_TRUE(cap.frames.empty());
    EXPECT_EQ(gw.sessionCount(), 1u) << "a malformed Disconnect closes nothing";
}


// ===========================================================================
// No-hang fix round 2 (I1): the egress spill — no best-effort fallback.
// ===========================================================================

// SpillFifo against a std::deque model: wraps, variable sizes, full → false.
TEST(ClientGatewayEgressRegression, SpillFifoMatchesAFifoModelAcrossWrapsRegression)
{
    SpillFifo f(4096);
    std::deque<std::pair<int64_t, std::vector<uint8_t>>> model;
    std::mt19937 rng(7);
    int64_t key = 0;
    for (int step = 0; step < 20000; ++step) {
        if (model.empty() || rng() % 3 != 0) {
            std::vector<uint8_t> v(rng() % 700);
            for (auto& b : v) b = static_cast<uint8_t>(rng());
            if (f.push(++key, v.data(), v.size())) model.emplace_back(key, std::move(v));
            else ASSERT_FALSE(model.empty()) << "an empty spill refused a record that fits";
        } else {
            const auto r = f.front();
            ASSERT_EQ(r.key, model.front().first);
            ASSERT_EQ(std::vector<uint8_t>(r.data, r.data + r.len), model.front().second);
            f.pop();
            model.pop_front();
        }
        ASSERT_EQ(f.count(), model.size());
    }
    const std::vector<uint8_t> huge(5000, 1);
    EXPECT_FALSE(f.push(1, huge.data(), huge.size())) << "a record larger than the spill must be refused";
}

// A back-pressured 4 KiB answer (larger than the old 512 B slot, which fell back
// to best-effort) is spilled and delivered once the publication clears; answers
// behind it keep their per-session order even when the offer would succeed.
TEST(ClientGatewayEgressRegression, LargeAnswerIsSpilledInOrderNotBestEffortRegression)
{
    CaptureAll cap;
    cap.backpressure = 1;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    const int64_t sid = gw.injectReliableTestSessionForTest(0);
    const std::vector<uint8_t> blob(4096, 0x5A);
    pc::TopicSchemaResponse big;
    big.sessionId = sid; big.correlationId = 1; big.topicId = 7; big.version = 1;
    big.descriptorBlob = blob.data(); big.descriptorLen = static_cast<uint32_t>(blob.size());
    std::vector<uint8_t> f1(big.encodedSize());
    big.encode(f1.data());
    pc::DefineTopicResponse small;
    small.sessionId = sid; small.correlationId = 2;
    std::vector<uint8_t> f2(small.encodedSize());
    small.encode(f2.data());

    EXPECT_TRUE(gw.sendControlFrame(sid, f1.data(), f1.size()));
    EXPECT_TRUE(gw.sendControlFrame(sid, f2.data(), f2.size()));   // the egress is clear again
    EXPECT_TRUE(cap.frames.empty()) << "the second answer overtook the spilled first one";
    ASSERT_EQ(gw.pendingResponseCount(), 2u);
    gw.drainPendingResponses(nowNs());
    ASSERT_EQ(cap.frames.size(), 2u);
    const auto r1 = pc::TopicSchemaResponse::decode(cap.frames[0].data(), cap.frames[0].size());
    ASSERT_TRUE(r1.has_value()) << "the 4 KiB answer was lost";
    EXPECT_EQ(r1->correlationId, 1);
    EXPECT_EQ(r1->descriptorLen, 4096u);
    const auto r2 = pc::DefineTopicResponse::decode(cap.frames[1].data(), cap.frames[1].size());
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->correlationId, 2);
    EXPECT_EQ(gw.pendingResponseCount(), 0u);
}

// The spill is full: the session is EXPIRED explicitly (SessionExpired + closed
// at the next drain — the client fails every pending op SessionLost); its
// spilled answers are discarded with it, other sessions' answers survive.
TEST(ClientGatewayEgressRegression, SpillFullExpiresTheSessionExplicitlyRegression)
{
    CaptureAll cap;
    cap.backpressure = 1 << 30;   // the publication never clears
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    const int64_t other = gw.injectReliableTestSessionForTest(0);
    const int64_t sid   = gw.injectReliableTestSessionForTest(0);
    pc::DefineTopicResponse o;
    o.sessionId = other; o.correlationId = 99;
    std::vector<uint8_t> fo(o.encodedSize());
    o.encode(fo.data());
    EXPECT_TRUE(gw.sendControlFrame(other, fo.data(), fo.size()));   // spilled first

    const std::vector<uint8_t> blob(64 * 1024, 0x11);
    pc::TopicSchemaResponse big;
    big.sessionId = sid; big.topicId = 7; big.version = 1;
    big.descriptorBlob = blob.data(); big.descriptorLen = static_cast<uint32_t>(blob.size());
    std::vector<uint8_t> f(big.encodedSize());
    for (int64_t corr = 1; corr <= 200; ++corr) {   // 200 x 64 KiB > the 4 MiB spill
        big.correlationId = corr;
        big.encode(f.data());
        (void)gw.sendControlFrame(sid, f.data(), f.size());
    }
    EXPECT_TRUE(gw.sessionExistsForTest(sid)) << "closed inside a handler (a caller may still hold it)";
    cap.backpressure = 0;
    gw.drainPendingResponses(nowNs());
    EXPECT_FALSE(gw.sessionExistsForTest(sid)) << "a session the spill could not hold was not expired";
    EXPECT_TRUE(gw.sessionExistsForTest(other));
    bool expiredSent = false, otherDelivered = false;
    for (const auto& fr : cap.frames) {
        if (auto e = pc::ErrorResponse::decode(fr.data(), fr.size()))
            expiredSent |= (e->sessionId == sid && e->code == static_cast<int16_t>(pc::ErrorCode::SessionExpired));
        if (auto d = pc::DefineTopicResponse::decode(fr.data(), fr.size())) otherDelivered |= d->correlationId == 99;
        if (auto t = pc::TopicSchemaResponse::decode(fr.data(), fr.size()))
            EXPECT_NE(t->sessionId, sid) << "an expired session's spilled answer was still sent";
    }
    EXPECT_TRUE(expiredSent) << "no SessionExpired for the expired session";
    EXPECT_TRUE(otherDelivered) << "another session's spilled answer was lost";
    EXPECT_EQ(gw.pendingResponseCount(), 0u);
}


// Fix round 2: a redirect endpoint too long for the frame is never dropped —
// the produce / subscribe is answered the bare NotLeader (client refreshes).
TEST(ClientGatewayEgressRegression, OversizedRedirectEndpointFallsBackToNotLeaderRegression)
{
    CaptureAll cap;
    const std::string hugeEndpoint(80 * 1024, 'h');   // > the 68 KiB egress scratch
    ClientGateway gw = ClientGateway::makeForTestWithLeaderView(
        cap.fn(), [&] { return LeaderView{/*isLeader=*/false, hugeEndpoint}; }, /*sessionId=*/1);
    std::vector<std::vector<uint8_t>> direct;
    gw.setOnEgressFrame([&](int64_t, const uint8_t* b, std::size_t n) { direct.emplace_back(b, b + n); });
    const int64_t sid = gw.injectReliableTestSessionForTest(0);
    auto batch = makeBatch();
    pc::Produce prod;
    prod.sessionId = sid; prod.correlationId = 71; prod.topicId = 0; prod.partition = 0;
    prod.partitionKnown = 1; prod.acks = 0; prod.producerId = -1;
    prod.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
    std::vector<uint8_t> frame(64 * 1024, 0);
    gw.applyProduceForTest(frame.data(), prod.encode(frame.data()));
    auto e = cap.last<pc::ErrorResponse>();
    ASSERT_TRUE(e.has_value()) << "a produce redirect with an oversized endpoint was dropped";
    EXPECT_EQ(e->correlationId, 71);
    EXPECT_EQ(e->code, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    EXPECT_TRUE(e->message.empty());

    pc::Subscribe sub;
    sub.sessionId = sid; sub.correlationId = 72; sub.topicId = 0; sub.partition = 0; sub.partitionKnown = 1;
    std::vector<uint8_t> sf(sub.encodedSize());
    sf.resize(sub.encode(sf.data()));
    gw.applySubscribeForTest(sf.data(), sf.size());
    e = cap.last<pc::ErrorResponse>();
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->correlationId, 72) << "a subscribe redirect with an oversized endpoint was dropped";
    EXPECT_EQ(e->code, static_cast<int16_t>(pc::ErrorCode::NotLeader));
}

// Fix round 2: an empty / too-short Produce batch is a malformed request —
// InvalidRequest (39, non-retriable), not Internal; the no-hang stats count
// the rejected and unanswerable frames.
TEST(ClientGatewayEgressRegression, EmptyProduceBatchIsInvalidRequestAndStatsCountRegression)
{
    CaptureAll cap;
    const fs::path dir = makeTempDir();
    std::vector<TopicSpec> specs;
    { TopicSpec ts; ts.name = "events"; ts.partitionCount = 1; ts.topicId = 0; specs.push_back(ts); }
    TopicRegistry registry(specs, dir.string());
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    gw.setTopicRegistry(&registry);
    const int64_t sid = gw.injectReliableTestSessionForTest(0);
    const std::vector<uint8_t> shortBatch(8, 0);
    for (const std::size_t n : {std::size_t{0}, shortBatch.size()}) {
        pc::Produce prod;
        prod.sessionId = sid; prod.correlationId = 80 + static_cast<int64_t>(n); prod.topicId = 0;
        prod.partition = 0; prod.partitionKnown = 1; prod.acks = 0; prod.producerId = -1;
        prod.recordBatch = std::span<const uint8_t>(shortBatch.data(), n);
        std::vector<uint8_t> frame(1024, 0);
        gw.applyProduceForTest(frame.data(), prod.encode(frame.data()));
        const auto r = cap.last<pc::ProduceResponse>();
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(r->correlationId, 80 + static_cast<int64_t>(n));
        EXPECT_EQ(r->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    }
    const auto f = headerOnlyFrame(pc::TemplateId::Produce, sid, 90, 24);
    gw.applyProduceForTest(f.data(), f.size());
    const auto u = headerOnlyFrame(pc::TemplateId::Produce, /*unknown sid=*/999999, 91, 24);
    gw.applyProduceForTest(u.data(), u.size());
    const auto st = gw.noHangStats();
    EXPECT_EQ(st.rejectedRequests, 1u);
    EXPECT_EQ(st.unanswerableFrames, 1u);
    std::error_code ec;
    fs::remove_all(dir, ec);
}


// ===========================================================================
// No-hang fix round 3 (I1): per-session spill fairness + explicit EXPIRING.
// ===========================================================================
namespace {
std::vector<uint8_t> defineAnswer(int64_t sid, int64_t corr) {
    pc::DefineTopicResponse r;
    r.sessionId = sid; r.correlationId = corr;
    std::vector<uint8_t> f(r.encodedSize());
    r.encode(f.data());
    return f;
}
std::vector<uint8_t> bigAnswer(int64_t sid, int64_t corr, std::size_t blobBytes) {
    static std::vector<uint8_t> blob;
    blob.assign(blobBytes, 0x33);
    pc::TopicSchemaResponse r;
    r.sessionId = sid; r.correlationId = corr; r.topicId = 1; r.version = 1;
    r.descriptorBlob = blob.data(); r.descriptorLen = static_cast<uint32_t>(blob.size());
    std::vector<uint8_t> f(r.encodedSize());
    r.encode(f.data());
    return f;
}
std::vector<int64_t> corrsOf(const CaptureAll& cap, int64_t sid) {
    std::vector<int64_t> out;
    for (const auto& f : cap.frames)
        if (const auto h = pc::MessageHeader::decode(f.data(), f.size()); h && h->sessionId == sid)
            out.push_back(h->correlationId);
    return out;
}
bool sessionExpiredSentTo(const CaptureAll& cap, int64_t sid) {
    for (const auto& f : cap.frames)
        if (auto e = pc::ErrorResponse::decode(f.data(), f.size());
            e && e->sessionId == sid && e->code == static_cast<int16_t>(pc::ErrorCode::SessionExpired))
            return true;
    return false;
}
} // namespace

// A permanently stalled session A must not block session B's spilled answers
// (the round-2 drain was one global FIFO that stopped at A's first refusal).
TEST(ClientGatewayEgressRegression, StalledSessionDoesNotBlockAnotherSessionsSpillRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    const int64_t a = gw.injectReliableTestSessionForTest(0);
    const int64_t b = gw.injectReliableTestSessionForTest(0);
    cap.blockedSids = {a, b};
    for (int64_t c = 1; c <= 3; ++c) {
        const auto fa = defineAnswer(a, c);
        ASSERT_TRUE(gw.sendControlFrame(a, fa.data(), fa.size()));
        const auto fb = defineAnswer(b, 100 + c);
        ASSERT_TRUE(gw.sendControlFrame(b, fb.data(), fb.size()));
    }
    ASSERT_EQ(gw.pendingResponseCount(), 6u);
    cap.blockedSids = {a};                     // B's publication clears; A stays stalled
    gw.drainPendingResponses(nowNs());
    EXPECT_EQ(corrsOf(cap, b), (std::vector<int64_t>{101, 102, 103})) << "B was blocked behind A (or reordered)";
    EXPECT_TRUE(corrsOf(cap, a).empty());
    EXPECT_EQ(gw.pendingResponseCount(), 3u);
    // A new answer for B goes straight out; a new one for A queues behind A's.
    const auto fb = defineAnswer(b, 104);
    ASSERT_TRUE(gw.sendControlFrame(b, fb.data(), fb.size()));
    const auto fa = defineAnswer(a, 4);
    ASSERT_TRUE(gw.sendControlFrame(a, fa.data(), fa.size()));
    cap.blockedSids.clear();
    gw.drainPendingResponses(nowNs());
    EXPECT_EQ(corrsOf(cap, a), (std::vector<int64_t>{1, 2, 3, 4})) << "A's per-session order was not preserved";
    EXPECT_EQ(corrsOf(cap, b), (std::vector<int64_t>{101, 102, 103, 104}));
    EXPECT_EQ(gw.pendingResponseCount(), 0u);
}

// The session over its spill quota is the one expired — never the innocent
// session that happens to push next.
TEST(ClientGatewayEgressRegression, SpillQuotaExpiresTheHogNotTheNextPusherRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    const int64_t hog = gw.injectReliableTestSessionForTest(0);
    const int64_t b   = gw.injectReliableTestSessionForTest(0);
    cap.blockedSids = {hog, b};
    int64_t corr = 0;
    while (gw.sessionExistsForTest(hog) && gw.noHangStats().spillExpiries == 0 && corr < 100) {
        const auto f = bigAnswer(hog, ++corr, 60 * 1024);
        (void)gw.sendControlFrame(hog, f.data(), f.size());
    }
    ASSERT_EQ(gw.noHangStats().spillExpiries, 1u) << "the hog never hit its quota";
    const auto fb = defineAnswer(b, 7);
    ASSERT_TRUE(gw.sendControlFrame(b, fb.data(), fb.size()));
    EXPECT_EQ(gw.noHangStats().spillExpiries, 1u) << "the innocent pusher was expired";
    cap.blockedSids.clear();
    gw.drainPendingResponses(nowNs());
    EXPECT_FALSE(gw.sessionExistsForTest(hog));
    EXPECT_TRUE(sessionExpiredSentTo(cap, hog));
    EXPECT_TRUE(gw.sessionExistsForTest(b));
    EXPECT_EQ(corrsOf(cap, b), (std::vector<int64_t>{7}));
}

// Globally full with every session under quota: the session holding the MOST
// spilled bytes is expired (and its space reclaimed), not the pusher.
TEST(ClientGatewayEgressRegression, GlobalSpillFullExpiresTheLargestHolderRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    std::vector<int64_t> holders;
    for (int i = 0; i < 8; ++i) holders.push_back(gw.injectReliableTestSessionForTest(0));
    const int64_t pusher = gw.injectReliableTestSessionForTest(0);
    cap.blockedSids = holders;
    cap.blockedSids.push_back(pusher);
    int64_t corr = 0;
    for (std::size_t i = 0; i < holders.size(); ++i) {   // 8 x ~490 KiB: under quota each, ~3.9 MiB total
        const std::size_t blob = (i == 3) ? 62 * 1024 : 60 * 1024;   // holder 3 is the largest
        for (int k = 0; k < 8; ++k) {
            const auto f = bigAnswer(holders[i], ++corr, blob);
            ASSERT_TRUE(gw.sendControlFrame(holders[i], f.data(), f.size()));
        }
    }
    ASSERT_EQ(gw.noHangStats().spillExpiries, 0u);
    for (int k = 0; k < 10; ++k) {                       // 10 x 40 KiB (under quota) overflow the spill
        const auto f = bigAnswer(pusher, 9000 + k, 40 * 1024);
        ASSERT_TRUE(gw.sendControlFrame(pusher, f.data(), f.size()));
    }
    EXPECT_GE(gw.noHangStats().spillExpiries, 1u);
    cap.blockedSids.clear();
    gw.drainPendingResponses(nowNs());
    EXPECT_FALSE(gw.sessionExistsForTest(holders[3])) << "the largest holder was not the one expired";
    EXPECT_TRUE(gw.sessionExistsForTest(pusher)) << "the pusher was expired instead of the largest holder";
    std::vector<int64_t> want;
    for (int64_t k = 0; k < 10; ++k) want.push_back(9000 + k);
    EXPECT_EQ(corrsOf(cap, pusher), want);
}

// The SessionExpired of an expiring session is RE-OFFERED until delivered (the
// round-2 close sent it once on the very publication that was back-pressured);
// the session is closed only once it is delivered.
TEST(ClientGatewayEgressRegression, ExpiringSessionKeepsOfferingSessionExpiredUntilDeliveredRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    gw.setNowForTest(1'000);
    const int64_t a = gw.injectReliableTestSessionForTest(0);
    cap.blockedSids = {a};
    for (int64_t c = 1; c <= 20; ++c) {                  // over its quota → EXPIRING
        const auto f = bigAnswer(a, c, 60 * 1024);
        (void)gw.sendControlFrame(a, f.data(), f.size());
    }
    ASSERT_EQ(gw.noHangStats().spillExpiring, 1u);
    for (int pass = 0; pass < 5; ++pass) gw.drainPendingResponses(2'000 + pass);   // still back-pressured
    EXPECT_TRUE(gw.sessionExistsForTest(a)) << "closed before its SessionExpired was delivered";
    EXPECT_FALSE(sessionExpiredSentTo(cap, a));
    EXPECT_EQ(gw.pendingResponseCount(), 0u) << "an expiring session's spilled answers are discarded";
    cap.blockedSids.clear();
    gw.drainPendingResponses(3'000);
    EXPECT_TRUE(sessionExpiredSentTo(cap, a));
    EXPECT_FALSE(gw.sessionExistsForTest(a));
    EXPECT_EQ(gw.noHangStats().spillExpiryNotified, 1u);
    EXPECT_EQ(gw.noHangStats().spillExpiring, 0u);
}

// ... and if the publication never clears, the session is closed at the lease
// deadline (counted as notify-lost: the client's own keepalive timeout fired).
TEST(ClientGatewayEgressRegression, ExpiringSessionIsClosedAtTheLeaseDeadlineRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    gw.setNowForTest(1'000);
    const int64_t a = gw.injectReliableTestSessionForTest(0);
    cap.blockedSids = {a};
    for (int64_t c = 1; c <= 20; ++c) {
        const auto f = bigAnswer(a, c, 60 * 1024);
        (void)gw.sendControlFrame(a, f.data(), f.size());
    }
    ASSERT_EQ(gw.noHangStats().spillExpiring, 1u);
    gw.drainPendingResponses(2'000);
    EXPECT_TRUE(gw.sessionExistsForTest(a));
    gw.drainPendingResponses(1'000 + 3'600'000'000'000LL);   // far past any lease
    EXPECT_FALSE(gw.sessionExistsForTest(a));
    EXPECT_EQ(gw.noHangStats().spillExpiryNotifyLost, 1u);
}


// Fix round 3: SpillFifo scan (skip / consume out of FIFO position), discard and
// compact against a model — live records always come back in push order.
TEST(ClientGatewayEgressRegression, SpillFifoScanDiscardCompactMatchModelRegression)
{
    SpillFifo f(8192, /*compactable=*/true);
    std::vector<std::pair<int64_t, std::vector<uint8_t>>> model;   // live records, push order
    std::mt19937 rng(11);
    int64_t seq = 0;
    for (int step = 0; step < 20000; ++step) {
        const unsigned op = rng() % 6;
        if (op <= 2) {
            std::vector<uint8_t> v(rng() % 300);
            for (auto& b : v) b = static_cast<uint8_t>(rng());
            const int64_t key = (++seq << 4) | static_cast<int64_t>(rng() % 4);   // 4 "sessions"
            if (f.push(key, v.data(), v.size())) model.emplace_back(key, std::move(v));
            else if (f.compact() && f.push(key, v.data(), v.size())) model.emplace_back(key, std::move(v));
        } else if (op == 3) {                     // consume every other record of one session
            const int64_t sess = static_cast<int64_t>(rng() % 4);
            bool take = true;
            std::vector<std::pair<int64_t, std::vector<uint8_t>>> keep;
            std::size_t idx = 0;
            f.scan([&](const SpillFifo::Record& r) {
                EXPECT_EQ(r.key, model[idx].first);
                EXPECT_EQ(std::vector<uint8_t>(r.data, r.data + r.len), model[idx].second);
                const bool consume = (r.key & 0xF) == sess && take;
                if ((r.key & 0xF) == sess) take = !take;
                if (!consume) keep.push_back(model[idx]);
                ++idx;
                return consume;
            });
            model.swap(keep);
        } else if (op == 4) {
            const int64_t sess = static_cast<int64_t>(rng() % 4);
            std::size_t n = 0;
            std::vector<std::pair<int64_t, std::vector<uint8_t>>> keep;
            for (auto& m : model) { if ((m.first & 0xF) == sess) ++n; else keep.push_back(std::move(m)); }
            std::size_t gone = 0;
            f.scan([&](const SpillFifo::Record& r) { if ((r.key & 0xF) == sess) { ++gone; return true; } return false; });
            EXPECT_EQ(gone, n);
            model.swap(keep);
        } else {
            ASSERT_TRUE(f.compact());
        }
        ASSERT_EQ(f.count(), model.size());
        if (!model.empty()) {
            const auto r = f.front();
            ASSERT_EQ(r.key, model.front().first);
        }
    }
}


// ===========================================================================
// Fix round 3: pre-session paths (host-resolve ring full, HandshakeFailed
// delivery, pending-connect eviction).
// ===========================================================================
namespace {
std::vector<uint8_t> connectFrame(int64_t corr, const std::string& channel) {
    pc::ConnectRequest req;
    req.correlationId = corr; req.egressStreamId = 9; req.clientVersion = pc::kClientVersion;
    req.egressChannel = channel;
    std::vector<uint8_t> f(1024);
    f.resize(req.encode(f.data()));
    return f;
}
} // namespace

// Concern 2: a momentarily full host-resolve ring kept the Connect pending (it
// used to be dropped) and the offer is retried until accepted.
TEST(ClientGatewayEgressRegression, FullHostResolveRingKeepsTheConnectPendingAndRetriesRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    rune::consensus::HostResolveRing req, resp;
    gw.setHostResolver(rune::consensus::HostResolvePort{&req, &resp});
    int filled = 0;
    while (req.offer(rune::consensus::kHostResolveRequest, -1, 0)) ++filled;   // the ring is full
    ASSERT_GT(filled, 0);
    const auto f = connectFrame(77, "aeron:udp?endpoint=client-host.example:40123");
    (void)gw.applyClientFrameForTest(f.data(), f.size());
    ASSERT_EQ(gw.pendingConnectCount(), 1u) << "a full resolve ring dropped the Connect";
    gw.pollPendingConnectsForTest(1);                     // still full: stays pending
    ASSERT_EQ(gw.pendingConnectCount(), 1u);
    int drained = 0, ours = 0;
    req.poll([&](std::int32_t, std::int64_t id, std::int64_t host) {
        ++drained;
        if (id != -1) { ++ours; delete reinterpret_cast<std::string*>(static_cast<std::intptr_t>(host)); }
    });
    EXPECT_EQ(ours, 0);
    gw.pollPendingConnectsForTest(2);                     // the retry is accepted now
    std::string offeredHost;
    req.poll([&](std::int32_t type, std::int64_t, std::int64_t host) {
        if (type != rune::consensus::kHostResolveRequest) return;
        auto* h = reinterpret_cast<std::string*>(static_cast<std::intptr_t>(host));
        offeredHost = *h;
        delete h;
    });
    EXPECT_EQ(offeredHost, "client-host.example") << "the refused resolve offer was never retried";
    EXPECT_EQ(gw.pendingConnectCount(), 1u) << "the connect must stay pending for its resolution";
}

// N4: HandshakeFailed is delivered reliably — a back-pressured offer keeps the
// failing handshake and re-offers it on the next poll.
TEST(ClientGatewayEgressRegression, HandshakeFailedIsReofferedUntilDeliveredRegression)
{
    CaptureAll cap;
    cap.backpressure = 1;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    gw.injectPendingHandshakeForTest(/*sessionId=*/4343);
    const auto f = headerOnlyFrame(pc::TemplateId::HandshakeContinue, 4343, 616, 32);
    (void)gw.applyClientFrameForTest(f.data(), f.size());
    EXPECT_FALSE(cap.last<pc::HandshakeContinueResponse>().has_value());
    ASSERT_EQ(gw.pendingConnectCount(), 1u) << "a back-pressured HandshakeFailed was dropped";
    gw.pollPendingConnectsForTest(1);
    const auto r = cap.last<pc::HandshakeContinueResponse>();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->correlationId, 616);
    EXPECT_EQ(r->code, static_cast<int16_t>(pc::ErrorCode::HandshakeFailed));
    EXPECT_EQ(gw.pendingConnectCount(), 0u);
}

// Fix-on-sight: a pending connect evicted by the cap is ANSWERED
// (ConnectResponse{Internal}, no session) when its reply publication is resolved.
TEST(ClientGatewayEgressRegression, EvictedPendingConnectIsAnsweredRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    rune::consensus::HostResolveRing req, resp;
    gw.setHostResolver(rune::consensus::HostResolvePort{&req, &resp});
    gw.injectPendingHandshakeForTest(/*sessionId=*/5001);   // the oldest: resolved reply publication
    int64_t corr = 1;
    while (cap.last<pc::ConnectResponse>() == std::nullopt && corr < 10'000) {
        const auto f = connectFrame(++corr, "aeron:udp?endpoint=client-host.example:40123");
        (void)gw.applyClientFrameForTest(f.data(), f.size());
        req.poll([](std::int32_t, std::int64_t, std::int64_t host) {
            delete reinterpret_cast<std::string*>(static_cast<std::intptr_t>(host));
        });
    }
    const auto r = cap.last<pc::ConnectResponse>();
    ASSERT_TRUE(r.has_value()) << "an evicted pending connect was dropped without an answer";
    EXPECT_EQ(r->code, static_cast<int16_t>(pc::ErrorCode::Internal));
    EXPECT_EQ(gw.sessionCount(), 0u);
}


// Fix round 3 (counter export): the no-hang counters are logged while the broker
// runs — on change, at most every kNoHangStatsLogNs — not only at shutdown.
TEST(ClientGatewayEgressRegression, NoHangStatsAreLoggedPeriodicallyOnChangeRegression)
{
    CaptureAll cap;
    ClientGateway gw = ClientGateway::makeForTest(cap.fn(), /*sessionId=*/1);
    const int64_t sid = gw.injectReliableTestSessionForTest(0);
    EXPECT_FALSE(gw.logNoHangStatsForTest(1)) << "nothing changed: no line";
    const auto f = headerOnlyFrame(pc::TemplateId::Produce, sid, 1, 24);
    gw.applyProduceForTest(f.data(), f.size());
    EXPECT_TRUE(gw.logNoHangStatsForTest(2)) << "a rejected request was not reported";
    EXPECT_EQ(gw.nextNoHangStatsLogNsForTest(), 2 + ClientGateway::kNoHangStatsLogNs);
    EXPECT_FALSE(gw.logNoHangStatsForTest(3)) << "unchanged counters logged again";
}

} // namespace
} // namespace rune::gateway
