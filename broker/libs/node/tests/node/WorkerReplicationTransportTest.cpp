// ---------------------------------------------------------------------------
// WorkerReplicationTransportTest.cpp — unit tests for WorkerReplicationTransport
//                                      send path (Task 3, E5 per-worker replication).
//
// Test strategy (Aeron-free seam):
//   We inject a SharedFakePubRegistry instead of AeronPubRegistry.
//   SharedFakePubRegistry records every sendToPeer / broadcastToPeers call
//   (NodeId + encoded bytes) via a shared_ptr<SharedState> so the observation
//   handle and the transport's internal copy share the same state.
//
//   Tests assert:
//     (a) Each targeted send goes to the correct peer NodeId only.
//     (b) sendFetchRequest broadcasts to ALL registered peers.
//     (c) Encoded bytes are SBE-correct (round-trip decode matches input).
//     (d) Reconcile variants carry the reconcile templateId (not the fetch one).
//     (e) resolved() reflects whether allResolved() on the registry is true.
//     (f) Back-pressure (sendToPeer ≤ 0) — nothing recorded, no crash.
//     (g) Unknown-peer send — nothing recorded to any peer, no crash.
//
// No Aeron driver required — SharedFakePubRegistry::allResolved() returns true
// once constructed with ≥1 peer.  No network, no file-system I/O.
// ---------------------------------------------------------------------------

#include <rune/node/WorkerReplicationTransport.h>
#include <rune/replication/ReplicationMessages.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace rune::replication;
using namespace rune::node;

// ===========================================================================
// SharedState — backing state shared between the observation handle and the
// registry copy stored inside the transport.
// ===========================================================================
struct SharedState {
    std::vector<NodeId>                                           peers;
    std::unordered_map<NodeId, std::vector<std::vector<uint8_t>>> offersTo;

    // When backPressurePeer is set, sendToPeer returns -1 for that peer (no record).
    NodeId backPressurePeer = static_cast<NodeId>(-1);

    explicit SharedState(std::vector<NodeId> p) : peers(std::move(p)) {
        for (NodeId id : peers) offersTo[id];  // ensure entry exists even if nothing sent
    }
};

// ===========================================================================
// SharedFakePubRegistry — test PubRegistry policy backed by a shared_ptr so
// the observation handle and the moved-into-transport copy see the same state.
//
// Satisfies the PubRegistry concept:
//   int64_t sendToPeer(NodeId, const uint8_t*, size_t)
//   void    broadcastToPeers(const uint8_t*, size_t)
//   bool    allResolved()
// ===========================================================================
class SharedFakePubRegistry {
public:
    explicit SharedFakePubRegistry(std::vector<NodeId> peers)
        : state_(std::make_shared<SharedState>(std::move(peers)))
    {}

    // Allow copy+move (shared_ptr semantics — all copies share the same state).
    SharedFakePubRegistry(SharedFakePubRegistry&&)            = default;
    SharedFakePubRegistry& operator=(SharedFakePubRegistry&&) = default;
    SharedFakePubRegistry(const SharedFakePubRegistry&)        = default;
    SharedFakePubRegistry& operator=(const SharedFakePubRegistry&) = default;

    [[nodiscard]] bool allResolved() const noexcept { return !state_->peers.empty(); }

    int64_t sendToPeer(NodeId peer, const uint8_t* buf, std::size_t len) noexcept
    {
        // Simulate back-pressure for a chosen peer — return ≤ 0, record nothing.
        if (peer == state_->backPressurePeer) return -1;

        // Unknown peer (not in the registered set) — return -1, record nothing.
        if (state_->offersTo.find(peer) == state_->offersTo.end()) return -1;

        state_->offersTo[peer].emplace_back(buf, buf + len);
        return 1;
    }

    void broadcastToPeers(const uint8_t* buf, std::size_t len) noexcept
    {
        for (NodeId id : state_->peers) sendToPeer(id, buf, len);
    }

    // -----------------------------------------------------------------------
    // Observation interface
    // -----------------------------------------------------------------------
    [[nodiscard]] const std::vector<std::vector<uint8_t>>& offersTo(NodeId peer) const
    {
        static const std::vector<std::vector<uint8_t>> empty;
        auto it = state_->offersTo.find(peer);
        return (it != state_->offersTo.end()) ? it->second : empty;
    }

    [[nodiscard]] const std::vector<NodeId>& peers() const noexcept { return state_->peers; }

    void clear() { for (auto& [id, v] : state_->offersTo) v.clear(); }

    // Configure back-pressure on a peer (subsequent sendToPeer to that peer returns -1).
    void setBackPressurePeer(NodeId peer) noexcept { state_->backPressurePeer = peer; }
    void clearBackPressure()              noexcept { state_->backPressurePeer = static_cast<NodeId>(-1); }

private:
    std::shared_ptr<SharedState> state_;
};

using SharedTestTransport = WorkerReplicationTransportImpl<SharedFakePubRegistry>;

// Helper: make a SharedFakePubRegistry + SharedTestTransport sharing state.
struct TestHarness {
    SharedFakePubRegistry               pubs;
    std::unique_ptr<SharedTestTransport> transport;
};

static TestHarness makeTestPair(int32_t workerId, std::vector<NodeId> peers)
{
    SharedFakePubRegistry pubs(peers);
    SharedFakePubRegistry pubsCopy = pubs;  // shared_ptr copy — same underlying state
    auto transport = std::make_unique<SharedTestTransport>(workerId, std::move(pubsCopy));
    return TestHarness{std::move(pubs), std::move(transport)};
}

// ---------------------------------------------------------------------------
// Encode helpers (reference encodings for byte-identity checks).
// ---------------------------------------------------------------------------

static std::vector<uint8_t> encodeExpectedFetchResponse(const FetchResponseMsg& resp)
{
    const std::size_t sz = FetchResponseMsg::encodedSize(resp.records.size());
    std::vector<uint8_t> buf(sz, 0);
    FetchResponseMsg::encode(buf.data(), 0,
                             resp.leaderEpoch, resp.partition, resp.highWatermark,
                             resp.logStartOffset, resp.errorCode, resp.records,
                             resp.topicId);
    return buf;
}

static std::vector<uint8_t> encodeExpectedFetchRequest(const FetchRequestMsg& req)
{
    std::vector<uint8_t> buf(FetchRequestMsg::TOTAL_SIZE, 0);
    FetchRequestMsg::encode(buf.data(), 0,
                            req.leaderEpoch, req.partition, req.followerId,
                            req.fetchOffset, req.maxBytes, req.topicId);
    return buf;
}

static std::vector<uint8_t> encodeExpectedOleReq(const OffsetsForLeaderEpochReqMsg& req)
{
    std::vector<uint8_t> buf(OffsetsForLeaderEpochReqMsg::TOTAL_SIZE, 0);
    OffsetsForLeaderEpochReqMsg::encode(buf.data(), 0,
                                        req.currentEpoch, req.partition,
                                        req.followerId, req.requestedEpoch,
                                        req.followerLeo, req.topicId);
    return buf;
}

static std::vector<uint8_t> encodeExpectedOleResp(const OffsetsForLeaderEpochRespMsg& resp)
{
    std::vector<uint8_t> buf(OffsetsForLeaderEpochRespMsg::TOTAL_SIZE, 0);
    OffsetsForLeaderEpochRespMsg::encode(buf.data(), 0,
                                         resp.leaderEpoch, resp.partition,
                                         resp.responseEpoch, resp.endOffset,
                                         resp.errorCode, resp.topicId);
    return buf;
}

// ===========================================================================
// 1. sendFetchResponse selects the correct peer pub and encodes the frame.
// ===========================================================================
TEST(WorkerReplicationTransport, SendFetchResponseSelectsPeerPubAndEncodesFrame)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    const std::vector<uint8_t> recordData(1024, 0xAB);
    FetchResponseMsg resp{};
    resp.leaderEpoch    = 7;
    resp.partition      = 5;
    resp.highWatermark  = 200;
    resp.logStartOffset = 50;
    resp.errorCode      = 0;
    resp.topicId        = 1;
    resp.records        = std::span<const uint8_t>(recordData);

    h.transport->sendFetchResponse(/*toFollower=*/3, resp);

    ASSERT_EQ(h.pubs.offersTo(3).size(), 1u)
        << "sendFetchResponse should send exactly one frame to the target follower";
    EXPECT_TRUE(h.pubs.offersTo(2).empty())
        << "sendFetchResponse must NOT send to non-target peer";

    const auto& frame = h.pubs.offersTo(3)[0];
    const FetchResponseMsg decoded = FetchResponseMsg::decode(frame.data(), 0, frame.size());
    EXPECT_EQ(decoded.leaderEpoch,    resp.leaderEpoch);
    EXPECT_EQ(decoded.partition,      resp.partition);
    EXPECT_EQ(decoded.highWatermark,  resp.highWatermark);
    EXPECT_EQ(decoded.logStartOffset, resp.logStartOffset);
    EXPECT_EQ(decoded.errorCode,      resp.errorCode);
    EXPECT_EQ(decoded.topicId,        resp.topicId);
    ASSERT_EQ(decoded.records.size(), recordData.size());
    EXPECT_EQ(0, std::memcmp(decoded.records.data(), recordData.data(), recordData.size()));

    EXPECT_EQ(peekTemplateId(frame.data(), 0),
              static_cast<uint16_t>(FetchResponseMsg::TEMPLATE_ID));

    const auto expected = encodeExpectedFetchResponse(resp);
    ASSERT_EQ(frame.size(), expected.size());
    EXPECT_EQ(0, std::memcmp(frame.data(), expected.data(), expected.size()))
        << "Frame bytes must be identical to the reference SBE encoding";
}

// ===========================================================================
// 2. sendFetchRequest broadcasts to ALL peers.
// ===========================================================================
TEST(WorkerReplicationTransport, SendFetchRequestBroadcastsToAllPeers)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    FetchRequestMsg req{};
    req.leaderEpoch = 3;
    req.partition   = 0;
    req.followerId  = 1;
    req.fetchOffset = 500;
    req.maxBytes    = 65536;
    req.topicId     = 0;

    h.transport->sendFetchRequest(req);

    ASSERT_EQ(h.pubs.offersTo(2).size(), 1u) << "sendFetchRequest must broadcast to peer 2";
    ASSERT_EQ(h.pubs.offersTo(3).size(), 1u) << "sendFetchRequest must broadcast to peer 3";

    const auto& frame2 = h.pubs.offersTo(2)[0];
    const FetchRequestMsg decoded = FetchRequestMsg::decode(frame2.data(), 0);
    EXPECT_EQ(decoded.leaderEpoch, req.leaderEpoch);
    EXPECT_EQ(decoded.partition,   req.partition);
    EXPECT_EQ(decoded.followerId,  req.followerId);
    EXPECT_EQ(decoded.fetchOffset, req.fetchOffset);
    EXPECT_EQ(decoded.maxBytes,    req.maxBytes);
    EXPECT_EQ(decoded.topicId,     req.topicId);

    EXPECT_EQ(peekTemplateId(frame2.data(), 0),
              static_cast<uint16_t>(FetchRequestMsg::TEMPLATE_ID));

    const auto expected = encodeExpectedFetchRequest(req);
    ASSERT_EQ(frame2.size(), expected.size());
    EXPECT_EQ(0, std::memcmp(frame2.data(), expected.data(), expected.size()));

    const auto& frame3 = h.pubs.offersTo(3)[0];
    ASSERT_EQ(frame2.size(), frame3.size());
    EXPECT_EQ(0, std::memcmp(frame2.data(), frame3.data(), frame2.size()))
        << "Broadcast frames must be identical";
}

// ===========================================================================
// 3. sendOffsetsForLeaderEpochRequest routes to the correct peer.
// ===========================================================================
TEST(WorkerReplicationTransport, SendOleRequestRoutesToLeader)
{
    auto h = makeTestPair(/*workerId=*/2, {2, 3});

    OffsetsForLeaderEpochReqMsg req{};
    req.currentEpoch   = 5;
    req.partition      = 1;
    req.followerId     = 4;
    req.requestedEpoch = 3;
    req.followerLeo    = 999;
    req.topicId        = 0;

    h.transport->sendOffsetsForLeaderEpochRequest(/*toLeader=*/2, req);

    ASSERT_EQ(h.pubs.offersTo(2).size(), 1u);
    EXPECT_TRUE(h.pubs.offersTo(3).empty());

    const auto& frame = h.pubs.offersTo(2)[0];
    const OffsetsForLeaderEpochReqMsg decoded =
        OffsetsForLeaderEpochReqMsg::decode(frame.data(), 0);
    EXPECT_EQ(decoded.currentEpoch,   req.currentEpoch);
    EXPECT_EQ(decoded.partition,      req.partition);
    EXPECT_EQ(decoded.followerId,     req.followerId);
    EXPECT_EQ(decoded.requestedEpoch, req.requestedEpoch);
    EXPECT_EQ(decoded.followerLeo,    req.followerLeo);
    EXPECT_EQ(decoded.topicId,        req.topicId);

    EXPECT_EQ(peekTemplateId(frame.data(), 0),
              static_cast<uint16_t>(OffsetsForLeaderEpochReqMsg::TEMPLATE_ID));

    const auto expected = encodeExpectedOleReq(req);
    ASSERT_EQ(frame.size(), expected.size());
    EXPECT_EQ(0, std::memcmp(frame.data(), expected.data(), expected.size()));
}

// ===========================================================================
// 4. sendOffsetsForLeaderEpochResponse routes to the correct peer.
// ===========================================================================
TEST(WorkerReplicationTransport, SendOleResponseRoutesToFollower)
{
    auto h = makeTestPair(/*workerId=*/2, {2, 3});

    OffsetsForLeaderEpochRespMsg resp{};
    resp.leaderEpoch   = 6;
    resp.partition     = 2;
    resp.responseEpoch = 4;
    resp.endOffset     = 888;
    resp.errorCode     = 0;
    resp.topicId       = 0;

    h.transport->sendOffsetsForLeaderEpochResponse(/*toFollower=*/3, resp);

    ASSERT_EQ(h.pubs.offersTo(3).size(), 1u);
    EXPECT_TRUE(h.pubs.offersTo(2).empty());

    const auto& frame = h.pubs.offersTo(3)[0];
    const OffsetsForLeaderEpochRespMsg decoded =
        OffsetsForLeaderEpochRespMsg::decode(frame.data(), 0);
    EXPECT_EQ(decoded.leaderEpoch,   resp.leaderEpoch);
    EXPECT_EQ(decoded.partition,     resp.partition);
    EXPECT_EQ(decoded.responseEpoch, resp.responseEpoch);
    EXPECT_EQ(decoded.endOffset,     resp.endOffset);
    EXPECT_EQ(decoded.errorCode,     resp.errorCode);
    EXPECT_EQ(decoded.topicId,       resp.topicId);

    EXPECT_EQ(peekTemplateId(frame.data(), 0),
              static_cast<uint16_t>(OffsetsForLeaderEpochRespMsg::TEMPLATE_ID));

    const auto expected = encodeExpectedOleResp(resp);
    ASSERT_EQ(frame.size(), expected.size());
    EXPECT_EQ(0, std::memcmp(frame.data(), expected.data(), expected.size()));
}

// ===========================================================================
// 5. sendReconcileRequest encodes FetchRequest with ReconcileFetchReq templateId.
// ===========================================================================
TEST(WorkerReplicationTransport, SendReconcileRequestStampsReconcileTemplateId)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    FetchRequestMsg req{};
    req.leaderEpoch = 4;
    req.partition   = 0;
    req.followerId  = 1;
    req.fetchOffset = 300;
    req.maxBytes    = 32768;
    req.topicId     = 0;

    h.transport->sendReconcileRequest(/*toPeer=*/2, req);

    ASSERT_EQ(h.pubs.offersTo(2).size(), 1u);
    EXPECT_TRUE(h.pubs.offersTo(3).empty());

    const auto& frame = h.pubs.offersTo(2)[0];

    EXPECT_EQ(peekTemplateId(frame.data(), 0), kReconcileFetchReqTemplateId)
        << "sendReconcileRequest must re-stamp the templateId to ReconcileFetchReq";
    EXPECT_NE(peekTemplateId(frame.data(), 0),
              static_cast<uint16_t>(FetchRequestMsg::TEMPLATE_ID));

    const FetchRequestMsg decoded = FetchRequestMsg::decode(frame.data(), 0);
    EXPECT_EQ(decoded.leaderEpoch, req.leaderEpoch);
    EXPECT_EQ(decoded.partition,   req.partition);
    EXPECT_EQ(decoded.followerId,  req.followerId);
    EXPECT_EQ(decoded.fetchOffset, req.fetchOffset);
    EXPECT_EQ(decoded.maxBytes,    req.maxBytes);
    EXPECT_EQ(decoded.topicId,     req.topicId);
}

// ===========================================================================
// 6. sendReconcileResponse encodes FetchResponse with ReconcileFetchResp templateId.
// ===========================================================================
TEST(WorkerReplicationTransport, SendReconcileResponseStampsReconcileTemplateId)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    const std::vector<uint8_t> recordData(256, 0xCD);
    FetchResponseMsg resp{};
    resp.leaderEpoch    = 9;
    resp.partition      = 3;
    resp.highWatermark  = 500;
    resp.logStartOffset = 100;
    resp.errorCode      = 0;
    resp.topicId        = 2;
    resp.records        = std::span<const uint8_t>(recordData);

    h.transport->sendReconcileResponse(/*toPeer=*/3, resp);

    ASSERT_EQ(h.pubs.offersTo(3).size(), 1u);
    EXPECT_TRUE(h.pubs.offersTo(2).empty());

    const auto& frame = h.pubs.offersTo(3)[0];

    EXPECT_EQ(peekTemplateId(frame.data(), 0), kReconcileFetchRespTemplateId)
        << "sendReconcileResponse must re-stamp the templateId to ReconcileFetchResp";
    EXPECT_NE(peekTemplateId(frame.data(), 0),
              static_cast<uint16_t>(FetchResponseMsg::TEMPLATE_ID));

    const FetchResponseMsg decoded = FetchResponseMsg::decode(frame.data(), 0, frame.size());
    EXPECT_EQ(decoded.leaderEpoch,    resp.leaderEpoch);
    EXPECT_EQ(decoded.partition,      resp.partition);
    EXPECT_EQ(decoded.highWatermark,  resp.highWatermark);
    EXPECT_EQ(decoded.logStartOffset, resp.logStartOffset);
    EXPECT_EQ(decoded.errorCode,      resp.errorCode);
    EXPECT_EQ(decoded.topicId,        resp.topicId);
    ASSERT_EQ(decoded.records.size(), recordData.size());
    EXPECT_EQ(0, std::memcmp(decoded.records.data(), recordData.data(), recordData.size()));
}

// ===========================================================================
// 7. resolved() returns true when FakePubRegistry has peers.
// ===========================================================================
TEST(WorkerReplicationTransport, ResolvedTrueWhenFakeRegistryHasPeers)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});
    EXPECT_TRUE(h.transport->resolved());
}

// ===========================================================================
// 8. sendFetchResponse with empty records (HW-only update).
// ===========================================================================
TEST(WorkerReplicationTransport, SendFetchResponseEmptyRecords)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    FetchResponseMsg resp{};
    resp.leaderEpoch    = 1;
    resp.partition      = 0;
    resp.highWatermark  = 42;
    resp.logStartOffset = 0;
    resp.errorCode      = 0;
    resp.topicId        = 0;
    resp.records        = {};

    h.transport->sendFetchResponse(/*toFollower=*/2, resp);

    ASSERT_EQ(h.pubs.offersTo(2).size(), 1u);
    EXPECT_TRUE(h.pubs.offersTo(3).empty());

    const auto& frame = h.pubs.offersTo(2)[0];
    EXPECT_EQ(frame.size(), FetchResponseMsg::encodedSize(0));

    const FetchResponseMsg decoded = FetchResponseMsg::decode(frame.data(), 0, frame.size());
    EXPECT_EQ(decoded.highWatermark, resp.highWatermark);
    EXPECT_TRUE(decoded.records.empty());

    EXPECT_EQ(peekTemplateId(frame.data(), 0),
              static_cast<uint16_t>(FetchResponseMsg::TEMPLATE_ID));
}

// ===========================================================================
// 9. Regression: sendFetchRequest with 3 peers broadcasts to all 3.
// ===========================================================================
TEST(WorkerReplicationTransport, SendFetchRequestBroadcastsToThreePeers)
{
    auto h = makeTestPair(/*workerId=*/0, {1, 2, 3});

    FetchRequestMsg req{};
    req.leaderEpoch = 1;
    req.partition   = 0;
    req.followerId  = 0;
    req.fetchOffset = 0;
    req.maxBytes    = 4096;
    req.topicId     = 0;

    h.transport->sendFetchRequest(req);

    EXPECT_EQ(h.pubs.offersTo(1).size(), 1u);
    EXPECT_EQ(h.pubs.offersTo(2).size(), 1u);
    EXPECT_EQ(h.pubs.offersTo(3).size(), 1u);
}

// ===========================================================================
// 10. workerId() accessor returns the configured worker id.
// ===========================================================================
TEST(WorkerReplicationTransport, WorkerIdAccessor)
{
    auto h = makeTestPair(/*workerId=*/7, {2, 3});
    EXPECT_EQ(h.transport->workerId(), 7);
}

// ===========================================================================
// 11. Back-pressure: when sendToPeer returns ≤ 0 nothing is recorded and no
//     partial/corrupt state remains (the send is simply dropped).
// ===========================================================================
TEST(WorkerReplicationTransport, SendDroppedOnBackPressureNothingRecorded)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    // Arm back-pressure on peer 3.
    h.pubs.setBackPressurePeer(3);

    const std::vector<uint8_t> recordData(128, 0x55);
    FetchResponseMsg resp{};
    resp.leaderEpoch    = 2;
    resp.partition      = 1;
    resp.highWatermark  = 50;
    resp.logStartOffset = 0;
    resp.errorCode      = 0;
    resp.topicId        = 0;
    resp.records        = std::span<const uint8_t>(recordData);

    // Send to the back-pressured peer — must not crash and must record nothing.
    h.transport->sendFetchResponse(/*toFollower=*/3, resp);

    EXPECT_TRUE(h.pubs.offersTo(3).empty())
        << "Back-pressured send must not record any frame";
    EXPECT_TRUE(h.pubs.offersTo(2).empty())
        << "Non-target peer must also have no frame";

    // Also verify with a targeted fixed-size message (sendOffsetsForLeaderEpochRequest).
    OffsetsForLeaderEpochReqMsg req{};
    req.currentEpoch   = 1;
    req.partition      = 0;
    req.followerId     = 1;
    req.requestedEpoch = 0;
    req.followerLeo    = 0;
    req.topicId        = 0;
    h.transport->sendOffsetsForLeaderEpochRequest(/*toLeader=*/3, req);

    EXPECT_TRUE(h.pubs.offersTo(3).empty())
        << "Back-pressured OLE request must also be dropped without recording";

    // Clear back-pressure — next send to peer 3 should succeed.
    h.pubs.clearBackPressure();
    h.transport->sendFetchResponse(/*toFollower=*/3, resp);
    EXPECT_EQ(h.pubs.offersTo(3).size(), 1u)
        << "After clearing back-pressure, send to peer 3 should be recorded";
}

// ===========================================================================
// 12. Unknown-peer: a targeted send to a NodeId NOT registered → nothing is
//     recorded to any peer and there is no crash.
// ===========================================================================
TEST(WorkerReplicationTransport, SendToUnknownPeerNothingRecordedNoCrash)
{
    // Register only peers 2 and 3.
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    constexpr NodeId kUnknown = 99;

    // sendFetchResponse to an unregistered peer.
    const std::vector<uint8_t> recordData(64, 0xAA);
    FetchResponseMsg resp{};
    resp.leaderEpoch    = 5;
    resp.partition      = 0;
    resp.highWatermark  = 10;
    resp.logStartOffset = 0;
    resp.errorCode      = 0;
    resp.topicId        = 0;
    resp.records        = std::span<const uint8_t>(recordData);

    h.transport->sendFetchResponse(/*toFollower=*/kUnknown, resp);

    // No crash; nothing recorded to any registered peer.
    EXPECT_TRUE(h.pubs.offersTo(2).empty())
        << "Unknown-peer send must not bleed into peer 2";
    EXPECT_TRUE(h.pubs.offersTo(3).empty())
        << "Unknown-peer send must not bleed into peer 3";

    // Also verify with a reconcile response.
    h.transport->sendReconcileResponse(/*toPeer=*/kUnknown, resp);
    EXPECT_TRUE(h.pubs.offersTo(2).empty());
    EXPECT_TRUE(h.pubs.offersTo(3).empty());

    // Confirm registered peers still work after the unknown-peer calls.
    h.transport->sendFetchResponse(/*toFollower=*/2, resp);
    EXPECT_EQ(h.pubs.offersTo(2).size(), 1u)
        << "Registered peer must still receive frames after unknown-peer calls";
}
