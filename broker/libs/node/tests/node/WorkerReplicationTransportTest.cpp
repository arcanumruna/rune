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

#include <algorithm>
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

    // Fake maxMessageLength returned by maxMessageLength().
    // Default: 1 MiB (generous; set to a small value to test maxBytes capping).
    int32_t fakeMaxMessageLength = 1 << 20;  // 1 MiB

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

    [[nodiscard]] bool hasPeer(NodeId peer) const noexcept
    {
        return std::find(state_->peers.begin(), state_->peers.end(), peer) != state_->peers.end();
    }

    // maxMessageLength — returns a fixed large value (simulates a generous Aeron
    // term; override via state_->fakeMaxMessageLength for cap tests).
    [[nodiscard]] int32_t maxMessageLength() const noexcept
    {
        return state_->fakeMaxMessageLength;
    }

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

    int64_t sendGatherToPeer(NodeId peer, const uint8_t* prefix, std::size_t prefixLen,
                             std::span<const uint8_t> body) noexcept
    {
        std::vector<uint8_t> frame(prefix, prefix + prefixLen);
        frame.insert(frame.end(), body.begin(), body.end());
        return sendToPeer(peer, frame.data(), frame.size());
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

    // Configure fake maxMessageLength (to test maxBytes capping).
    void setFakeMaxMessageLength(int32_t v) noexcept { state_->fakeMaxMessageLength = v; }

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

// ===========================================================================
// Poll path tests (Task 4)
// ===========================================================================

// ---------------------------------------------------------------------------
// Helper: encode a FetchResponse into a vector<uint8_t> with the given
// partition, topicId, and records blob.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeFetchResponseFrame(PartitionId partition,
                                                    int32_t     topicId,
                                                    std::vector<uint8_t> records)
{
    FetchResponseMsg resp{};
    resp.leaderEpoch    = 1;
    resp.partition      = partition;
    resp.highWatermark  = 100;
    resp.logStartOffset = 0;
    resp.errorCode      = 0;
    resp.topicId        = topicId;
    resp.records        = std::span<const uint8_t>(records);

    const std::size_t sz = FetchResponseMsg::encodedSize(records.size());
    std::vector<uint8_t> buf(sz);
    FetchResponseMsg::encode(buf.data(), 0,
                             resp.leaderEpoch, resp.partition, resp.highWatermark,
                             resp.logStartOffset, resp.errorCode, resp.records,
                             resp.topicId);
    return buf;
}

// ---------------------------------------------------------------------------
// Helper: encode a FetchRequest into a vector<uint8_t>.
// ---------------------------------------------------------------------------
static std::vector<uint8_t> makeFetchRequestFrame(PartitionId partition,
                                                   int32_t     topicId,
                                                   int32_t     maxBytes)
{
    std::vector<uint8_t> buf(FetchRequestMsg::TOTAL_SIZE);
    FetchRequestMsg::encode(buf.data(), 0,
                            /*leaderEpoch=*/1, partition, /*followerId=*/2,
                            /*fetchOffset=*/0, maxBytes, topicId);
    return buf;
}

// ===========================================================================
// 13. poll() dispatches a FetchResponse frame inline with correct routing
//     fields and zero-copy records access in-callback.
// ===========================================================================
TEST(WorkerReplicationTransport, PollDispatchesFrameToOwningReplicaInline)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    // Build 1024-byte records blob.
    const std::vector<uint8_t> records(1024, 0xAB);
    constexpr PartitionId kPartition = 5;
    constexpr int32_t     kTopicId   = 7;

    // Encode and inject a FetchResponse frame.
    const auto frame = makeFetchResponseFrame(kPartition, kTopicId, records);
    h.transport->injectFrameForTest(frame.data(), frame.size());

    // Poll: dispatch must be called exactly once, in-callback.
    std::vector<int32_t>  seenPartitions;
    std::vector<int32_t>  seenTopicIds;
    std::vector<uint16_t> seenTemplateIds;
    std::size_t           seenRecordLen = 0;

    const int n = h.transport->poll(10,
        [&](int32_t topicId, int32_t part, uint16_t tmpl,
            const uint8_t* f, std::size_t len) {
            seenPartitions.push_back(part);
            seenTopicIds.push_back(topicId);
            seenTemplateIds.push_back(tmpl);
            // Decode records INSIDE the callback (zero-copy lifetime contract).
            const FetchResponseMsg decoded = FetchResponseMsg::decode(f, 0, len);
            seenRecordLen = decoded.records.size();
        });

    EXPECT_EQ(n, 1) << "poll must return 1 fragment handled";
    ASSERT_EQ(seenPartitions.size(), 1u);
    EXPECT_EQ(seenPartitions[0], kPartition);
    EXPECT_EQ(seenTopicIds[0],   kTopicId);
    EXPECT_EQ(seenTemplateIds[0],
              static_cast<uint16_t>(FetchResponseMsg::TEMPLATE_ID));
    EXPECT_EQ(seenRecordLen, records.size())
        << "records must be fully accessible inside the dispatch callback";
}

// ===========================================================================
// 14. poll() with fragmentLimit=0 processes nothing and returns 0.
// ===========================================================================
TEST(WorkerReplicationTransport, PollZeroFragmentLimitProcessesNothing)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    const std::vector<uint8_t> records(64, 0xCC);
    const auto frame = makeFetchResponseFrame(/*partition=*/0, /*topicId=*/0, records);
    h.transport->injectFrameForTest(frame.data(), frame.size());

    int callCount = 0;
    const int n = h.transport->poll(0,
        [&](int32_t, int32_t, uint16_t, const uint8_t*, std::size_t) {
            ++callCount;
        });

    EXPECT_EQ(n, 0)        << "fragmentLimit=0 must return 0";
    EXPECT_EQ(callCount, 0) << "dispatch must not be called when fragmentLimit=0";
}

// ===========================================================================
// 15. poll() dispatches all 6 message types with correct templateId/topicId/
//     partition routing fields extracted.
// ===========================================================================
TEST(WorkerReplicationTransport, PollDispatchesAllMessageTypes)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    // Inject one frame of each type.
    const std::vector<uint8_t> records(128, 0xDD);
    constexpr PartitionId kPart = 3;
    constexpr int32_t     kTopic = 9;

    // FetchRequest (templateId=1)
    {
        auto buf = makeFetchRequestFrame(kPart, kTopic, /*maxBytes=*/4096);
        h.transport->injectFrameForTest(buf.data(), buf.size());
    }
    // FetchResponse (templateId=2)
    {
        auto buf = makeFetchResponseFrame(kPart, kTopic, records);
        h.transport->injectFrameForTest(buf.data(), buf.size());
    }
    // OffsetsForLeaderEpochReq (templateId=3)
    {
        std::vector<uint8_t> buf(OffsetsForLeaderEpochReqMsg::TOTAL_SIZE);
        OffsetsForLeaderEpochReqMsg::encode(buf.data(), 0,
            /*currentEpoch=*/1, kPart, /*followerId=*/2,
            /*requestedEpoch=*/0, /*followerLeo=*/0, kTopic);
        h.transport->injectFrameForTest(buf.data(), buf.size());
    }
    // OffsetsForLeaderEpochResp (templateId=4)
    {
        std::vector<uint8_t> buf(OffsetsForLeaderEpochRespMsg::TOTAL_SIZE);
        OffsetsForLeaderEpochRespMsg::encode(buf.data(), 0,
            /*leaderEpoch=*/1, kPart, /*responseEpoch=*/0,
            /*endOffset=*/0, /*errorCode=*/0, kTopic);
        h.transport->injectFrameForTest(buf.data(), buf.size());
    }
    // ReconcileFetchReq (templateId=5) — FetchRequest re-stamped.
    {
        auto buf = makeFetchRequestFrame(kPart, kTopic, /*maxBytes=*/4096);
        stampTemplateId(buf.data(), 0, kReconcileFetchReqTemplateId);
        h.transport->injectFrameForTest(buf.data(), buf.size());
    }
    // ReconcileFetchResp (templateId=6) — FetchResponse re-stamped.
    {
        auto buf = makeFetchResponseFrame(kPart, kTopic, records);
        stampTemplateId(buf.data(), 0, kReconcileFetchRespTemplateId);
        h.transport->injectFrameForTest(buf.data(), buf.size());
    }

    struct Seen { int32_t topicId; int32_t partition; uint16_t templateId; };
    std::vector<Seen> seen;

    const int n = h.transport->poll(10,
        [&](int32_t tid, int32_t part, uint16_t tmpl,
            const uint8_t*, std::size_t) {
            seen.push_back({tid, part, tmpl});
        });

    ASSERT_EQ(n, 6) << "all 6 injected frames must be polled";
    ASSERT_EQ(seen.size(), 6u);

    const std::vector<uint16_t> expectedTemplates = {
        static_cast<uint16_t>(ReplicationTemplateId::FetchRequest),
        static_cast<uint16_t>(ReplicationTemplateId::FetchResponse),
        static_cast<uint16_t>(ReplicationTemplateId::OffsetsForLeaderEpochReq),
        static_cast<uint16_t>(ReplicationTemplateId::OffsetsForLeaderEpochResp),
        kReconcileFetchReqTemplateId,
        kReconcileFetchRespTemplateId,
    };
    for (std::size_t i = 0; i < seen.size(); ++i) {
        EXPECT_EQ(seen[i].templateId, expectedTemplates[i])
            << "templateId mismatch at index " << i;
        EXPECT_EQ(seen[i].partition, kPart)
            << "partition mismatch at index " << i;
        EXPECT_EQ(seen[i].topicId, kTopic)
            << "topicId mismatch at index " << i;
    }
}

// ===========================================================================
// 16. poll() caps FetchRequest.maxBytes to maxFetchResponseBytes() before
//     dispatch so the leader never builds an oversized FetchResponse.
//     (GR-1 carry-forward from Task 3 review.)
// ===========================================================================
TEST(WorkerReplicationTransport, PollCapsFetchRequestMaxBytes)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    // Compute what maxFetchResponseBytes() should return for this fake setup.
    // We want the cap to actually fire, so set a small fakeMaxMessageLength.
    // kFetchResponseOverhead = FetchResponseMsg::encodedSize(0).
    const int32_t kOverhead =
        static_cast<int32_t>(FetchResponseMsg::encodedSize(0));
    const int32_t kSmallMax  = kOverhead + 512;   // only 512 bytes of records fit
    h.pubs.setFakeMaxMessageLength(kSmallMax);
    // Re-create transport with the updated shared state (transport already shares state).
    const int32_t expectedCeiling = 512;

    // FetchRequest with maxBytes MUCH larger than the ceiling.
    constexpr int32_t kBigMaxBytes = 1 << 20;  // 1 MiB
    auto buf = makeFetchRequestFrame(/*partition=*/2, /*topicId=*/4, kBigMaxBytes);
    h.transport->injectFrameForTest(buf.data(), buf.size());

    int32_t dispatchedMaxBytes = -1;
    const int n = h.transport->poll(10,
        [&](int32_t, int32_t, uint16_t,
            const uint8_t* f, std::size_t len) {
            const FetchRequestMsg decoded = FetchRequestMsg::decode(f, 0);
            dispatchedMaxBytes = decoded.maxBytes;
        });

    EXPECT_EQ(n, 1);
    EXPECT_EQ(dispatchedMaxBytes, expectedCeiling)
        << "poll must cap maxBytes to maxFetchResponseBytes() = " << expectedCeiling;
}

// ===========================================================================
// 17. poll() does NOT cap FetchRequest.maxBytes when it is already ≤ ceiling.
// ===========================================================================
TEST(WorkerReplicationTransport, PollDoesNotCapFetchRequestMaxBytesWhenWithinBound)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    // Default fakeMaxMessageLength = 1 MiB; maxFetchResponseBytes() = 1 MiB - overhead.
    constexpr int32_t kSmallMaxBytes = 1024;  // well within 1 MiB ceiling
    auto buf = makeFetchRequestFrame(/*partition=*/1, /*topicId=*/0, kSmallMaxBytes);
    h.transport->injectFrameForTest(buf.data(), buf.size());

    int32_t dispatchedMaxBytes = -1;
    h.transport->poll(10,
        [&](int32_t, int32_t, uint16_t, const uint8_t* f, std::size_t) {
            dispatchedMaxBytes = FetchRequestMsg::decode(f, 0).maxBytes;
        });

    EXPECT_EQ(dispatchedMaxBytes, kSmallMaxBytes)
        << "maxBytes must be passed through unchanged when already within ceiling";
}

// ===========================================================================
// 18. poll() caps ReconcileFetchReq.maxBytes the same as FetchRequest.
// ===========================================================================
TEST(WorkerReplicationTransport, PollCapsReconcileFetchReqMaxBytes)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    const int32_t kOverhead  = static_cast<int32_t>(FetchResponseMsg::encodedSize(0));
    const int32_t kSmallMax  = kOverhead + 256;
    h.pubs.setFakeMaxMessageLength(kSmallMax);

    auto buf = makeFetchRequestFrame(/*partition=*/0, /*topicId=*/0, 1 << 18);
    stampTemplateId(buf.data(), 0, kReconcileFetchReqTemplateId);
    h.transport->injectFrameForTest(buf.data(), buf.size());

    int32_t dispatchedMaxBytes = -1;
    uint16_t dispatchedTemplate = 0;
    h.transport->poll(10,
        [&](int32_t, int32_t, uint16_t tmpl, const uint8_t* f, std::size_t) {
            dispatchedMaxBytes = FetchRequestMsg::decode(f, 0).maxBytes;
            dispatchedTemplate = tmpl;
        });

    EXPECT_EQ(dispatchedMaxBytes, 256)
        << "ReconcileFetchReq maxBytes must also be capped";
    EXPECT_EQ(dispatchedTemplate, kReconcileFetchReqTemplateId)
        << "ReconcileFetchReq templateId must be preserved after cap";
}

// ===========================================================================
// 19. Regression: back-to-back polls over a SINGLE shared backing buffer must
//     not alias records — dispatch MUST decode all needed bytes INSIDE the
//     callback (zero-copy lifetime contract).
//
//     Mechanism (real aliasing guard):
//       - A single std::vector<uint8_t> `sharedBuf` is used as the Aeron
//         term-buffer stand-in.
//       - Frame1 (pattern 0xAA) is encoded INTO sharedBuf; injected via
//         injectFrameViewForTest — which stores a RAW POINTER into sharedBuf,
//         NOT a copy.
//       - poll(1): dispatch is called with a pointer into sharedBuf; decoded
//         bytes are captured INSIDE the callback.
//       - After poll() returns, sharedBuf is OVERWRITTEN with frame2 (0xBB).
//       - Frame2 is injected (same sharedBuf, same address).
//       - poll(1): dispatch sees frame2 bytes.
//
//     Aliasing failure mode: if a broken dispatch implementation stored the raw
//     pointer from the first poll() and deferred reading it until after
//     sharedBuf was overwritten, firstByte1 would equal 0xBB instead of 0xAA,
//     causing EXPECT_EQ(firstByte1, 0xAA) to FAIL.  The current correct
//     in-callback decode passes because firstByte1 is captured before poll()
//     returns.
// ===========================================================================
TEST(WorkerReplicationTransportRegression, BackToBackPollsDoNotAliasRecords)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    const std::vector<uint8_t> records1(1024, 0xAA);
    const std::vector<uint8_t> records2(1024, 0xBB);

    const auto frame1 = makeFetchResponseFrame(/*partition=*/5, /*topicId=*/0, records1);
    const auto frame2 = makeFetchResponseFrame(/*partition=*/6, /*topicId=*/0, records2);

    // Single backing buffer — the Aeron term-buffer stand-in.  Both frames
    // will occupy the SAME memory (same allocation, same address) at different
    // points in time.  We reserve up front so subsequent resize() never
    // reallocates, and overwrite the bytes IN PLACE via memcpy.
    //
    // This is essential to the guard: a move-assign from a temporary (e.g.
    // `sharedBuf = makeFetchResponseFrame(...)`) would FREE the old block and
    // swap in a fresh allocation at a DIFFERENT address — a stale pointer would
    // then dangle into freed memory (UB), not deterministically read 0xBB, and
    // the test would be vacuous.  In-place overwrite keeps the address stable so
    // a deferred read reliably observes frame2's 0xBB bytes.
    std::vector<uint8_t> sharedBuf;
    sharedBuf.reserve(std::max(frame1.size(), frame2.size()));

    // --- Frame 1 (0xAA) --- write into sharedBuf IN PLACE, inject as a raw view.
    sharedBuf.resize(frame1.size());
    std::memcpy(sharedBuf.data(), frame1.data(), frame1.size());
    const uint8_t* const backingAddr = sharedBuf.data();  // stable across the test
    h.transport->injectFrameViewForTest(sharedBuf.data(), sharedBuf.size());

    uint8_t     firstByte1 = 0;
    PartitionId firstPart1 = -1;
    std::size_t firstLen1  = 0;
    const int n1 = h.transport->poll(1,
        [&](int32_t, int32_t part, uint16_t,
            const uint8_t* f, std::size_t len) {
            // MUST decode INSIDE the callback — pointer is valid ONLY here.
            // If decode is deferred past this callback, sharedBuf will have
            // been overwritten with frame2 and the assertion will catch it.
            const FetchResponseMsg decoded = FetchResponseMsg::decode(f, 0, len);
            firstByte1 = decoded.records.empty() ? 0 : decoded.records[0];
            firstPart1 = part;
            firstLen1  = decoded.records.size();
        });

    EXPECT_EQ(n1, 1) << "first poll must deliver exactly 1 fragment";
    EXPECT_EQ(firstPart1, 5);
    EXPECT_EQ(firstLen1, 1024u);
    EXPECT_EQ(firstByte1, 0xAA)
        << "first poll must see 0xAA; failure here = dispatch read the pointer "
           "AFTER sharedBuf was overwritten (aliasing bug)";

    // Overwrite sharedBuf with frame2 (0xBB) IN PLACE — same allocation, same
    // address — simulating the Aeron driver reusing the term-buffer slot for the
    // next frame.  A stale pointer from the first poll now reads frame2's bytes.
    sharedBuf.resize(frame2.size());
    std::memcpy(sharedBuf.data(), frame2.data(), frame2.size());
    ASSERT_EQ(sharedBuf.data(), backingAddr)
        << "backing buffer must NOT reallocate — the aliasing guard depends on the "
           "second frame occupying the SAME address as the first";
    h.transport->injectFrameViewForTest(sharedBuf.data(), sharedBuf.size());

    uint8_t     firstByte2 = 0;
    PartitionId firstPart2 = -1;
    const int n2 = h.transport->poll(1,
        [&](int32_t, int32_t part, uint16_t,
            const uint8_t* f, std::size_t len) {
            const FetchResponseMsg decoded = FetchResponseMsg::decode(f, 0, len);
            firstByte2 = decoded.records.empty() ? 0 : decoded.records[0];
            firstPart2 = part;
        });

    EXPECT_EQ(n2, 1) << "second poll must deliver exactly 1 fragment";
    EXPECT_EQ(firstPart2, 6);
    EXPECT_EQ(firstByte2, 0xBB)
        << "second poll must see the 0xBB pattern of frame2";

    // Third poll: queue is now empty.
    int callCount = 0;
    const int n3 = h.transport->poll(10,
        [&](int32_t, int32_t, uint16_t, const uint8_t*, std::size_t) {
            ++callCount;
        });
    EXPECT_EQ(n3, 0)        << "queue must be empty after consuming both frames";
    EXPECT_EQ(callCount, 0) << "dispatch must not be called on an empty queue";
}

// ===========================================================================
// 20. poll() returns 0 when the transport is not resolved (no peer pubs).
// ===========================================================================
TEST(WorkerReplicationTransport, PollReturnsZeroWhenNotResolved)
{
    // Construct a transport with NO peers → allResolved() returns false →
    // resolved() returns false → poll() must return 0 immediately.
    SharedFakePubRegistry noPeers({});  // empty peer list → not resolved
    auto transport = std::make_unique<SharedTestTransport>(/*workerId=*/1,
                                                           std::move(noPeers));

    ASSERT_FALSE(transport->resolved())
        << "pre-condition: transport must not be resolved with empty peer list";

    // Inject a valid frame — it must NOT be dispatched.
    const std::vector<uint8_t> records(64, 0xCC);
    const auto frame = makeFetchResponseFrame(/*partition=*/0, /*topicId=*/0, records);
    transport->injectFrameForTest(frame.data(), frame.size());

    int callCount = 0;
    const int n = transport->poll(10,
        [&](int32_t, int32_t, uint16_t, const uint8_t*, std::size_t) {
            ++callCount;
        });

    EXPECT_EQ(n, 0)        << "poll must return 0 when not resolved";
    EXPECT_EQ(callCount, 0) << "dispatch must not be called when not resolved";
}

// ===========================================================================
// 21. poll() drops frames with an unknown/garbage templateId (default: case)
//     without invoking dispatch and without crashing.
// ===========================================================================
TEST(WorkerReplicationTransport, PollDropsUnknownTemplateIdFrame)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    // Encode a valid FetchRequest to get the right overall size, then patch
    // the templateId to a value not in the enum (0xFFFF = garbage).
    auto buf = makeFetchRequestFrame(/*partition=*/0, /*topicId=*/0, /*maxBytes=*/4096);

    // Stamp an unrecognised templateId into the SBE message header.
    constexpr uint16_t kGarbageTemplateId = 0xFFFF;
    stampTemplateId(buf.data(), 0, kGarbageTemplateId);

    h.transport->injectFrameForTest(buf.data(), buf.size());

    int callCount = 0;
    const int n = h.transport->poll(10,
        [&](int32_t, int32_t, uint16_t, const uint8_t*, std::size_t) {
            ++callCount;
        });

    // poll() must return 1 (one fragment was processed — it was examined and
    // dropped), but dispatch must NOT have been invoked.
    EXPECT_EQ(n, 1)        << "poll must return the fragment count (frame was consumed)";
    EXPECT_EQ(callCount, 0) << "dispatch must not be called for an unknown templateId";
}

// ===========================================================================
// 22. poll() drops a truncated frame (shorter than the fixed block) without
//     crashing — the length guard in dispatchFrame_ must catch it.
// ===========================================================================
TEST(WorkerReplicationTransport, PollDropsTruncatedFrameWithoutCrash)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    // Build a valid full frame, then truncate it to just 4 bytes — far below
    // both the SBE header size and the fixed-block minimum.
    const std::vector<uint8_t> records(64, 0xAA);
    auto fullFrame = makeFetchResponseFrame(/*partition=*/1, /*topicId=*/0, records);

    // Inject only the first 4 bytes (header-only truncation).
    constexpr std::size_t kTruncLen = 4;
    ASSERT_GE(fullFrame.size(), kTruncLen);
    h.transport->injectFrameForTest(fullFrame.data(), kTruncLen);

    // Also inject a 0-byte frame to cover the degenerate empty case.
    h.transport->injectFrameForTest(fullFrame.data(), 0);

    int callCount = 0;
    const int n = h.transport->poll(10,
        [&](int32_t, int32_t, uint16_t, const uint8_t*, std::size_t) {
            ++callCount;
        });

    EXPECT_EQ(n, 2)        << "poll must count both truncated frames as processed";
    EXPECT_EQ(callCount, 0) << "dispatch must not be called for truncated frames";
}

TEST(WorkerReplicationTransportRegression, SendFetchRequestToUnicastsToLeaderOnly)
{
    auto h = makeTestPair(/*workerId=*/0, {2, 3});
    FetchRequestMsg req{4, 1, 7, 100, 0, 3};
    h.transport->sendFetchRequestTo(/*leader=*/2, req);
    ASSERT_EQ(h.pubs.offersTo(2).size(), 1u);
    EXPECT_TRUE(h.pubs.offersTo(3).empty()) << "fast-ack must not be broadcast";
    EXPECT_EQ(h.pubs.offersTo(2)[0], encodeExpectedFetchRequest(req));
}

TEST(WorkerReplicationTransport, SendFetchRequestToUnknownLeaderFallsBackToBroadcast)
{
    auto h = makeTestPair(/*workerId=*/0, {2, 3});
    FetchRequestMsg req{4, 1, 7, 100, 0, 3};
    h.transport->sendFetchRequestTo(/*leader=*/9, req);
    EXPECT_EQ(h.pubs.offersTo(2).size(), 1u);
    EXPECT_EQ(h.pubs.offersTo(3).size(), 1u);
}

TEST(WorkerReplicationTransportRegression, GatherSendIsByteIdenticalForLargeRecords)
{
    auto h = makeTestPair(/*workerId=*/0, {2});
    const std::vector<uint8_t> big(64 * 1024, 0xC3);
    FetchResponseMsg resp{};
    resp.leaderEpoch = 3; resp.partition = 1; resp.highWatermark = 9;
    resp.errorCode = 0; resp.topicId = 4; resp.records = std::span<const uint8_t>(big);
    h.transport->sendFetchResponse(2, resp);
    ASSERT_EQ(h.pubs.offersTo(2).size(), 1u);
    EXPECT_EQ(h.pubs.offersTo(2)[0], encodeExpectedFetchResponse(resp));
    h.pubs.clear();
    h.transport->sendReconcileResponse(2, resp);
    ASSERT_EQ(h.pubs.offersTo(2).size(), 1u);
    EXPECT_EQ(peekTemplateId(h.pubs.offersTo(2)[0].data(), 0), kReconcileFetchRespTemplateId);
}
