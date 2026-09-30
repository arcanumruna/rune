// ---------------------------------------------------------------------------
// WorkerReplicationTransportTest.cpp — unit tests for WorkerReplicationTransport
//                                      send path (Task 3, E5 per-worker replication).
//
// Test strategy (Aeron-free seam):
//   We inject a FakePubRegistry instead of AeronPubRegistry.  FakePubRegistry
//   records every sendToPeer / broadcastToPeers call (NodeId + encoded bytes).
//   WorkerReplicationTransportImpl<FakePubRegistry> is alias TestTransport.
//
//   Tests assert:
//     (a) Each targeted send goes to the correct peer NodeId only.
//     (b) sendFetchRequest broadcasts to ALL registered peers.
//     (c) Encoded bytes are SBE-correct (round-trip decode matches input).
//     (d) Reconcile variants carry the reconcile templateId (not the fetch one).
//     (e) resolved() reflects whether allResolved() on the registry is true.
//
// No Aeron driver required — FakePubRegistry::allResolved() always returns
// true once constructed with ≥1 peer.  No network, no file-system I/O.
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
// FakePubRegistry — test-only PubRegistry policy.
//
// Records all sendToPeer / broadcastToPeers calls so tests can inspect what
// the transport would have sent.  Satisfies the PubRegistry concept:
//   int64_t sendToPeer(NodeId, const uint8_t*, size_t)
//   void    broadcastToPeers(const uint8_t*, size_t)
//   bool    allResolved()
// ===========================================================================
class FakePubRegistry {
public:
    struct Offer {
        std::vector<uint8_t> bytes;
    };

    // Construct with known peer NodeIds (determines allResolved and broadcast targets).
    explicit FakePubRegistry(std::vector<NodeId> peers) noexcept
        : peers_(std::move(peers))
    {
        for (NodeId id : peers_) {
            offers_[id]; // ensure entry exists even if nothing sent yet
        }
    }

    // FakePubRegistry is move-constructible so it can be passed to the test ctor.
    FakePubRegistry(FakePubRegistry&&)                 = default;
    FakePubRegistry& operator=(FakePubRegistry&&)      = default;
    FakePubRegistry(const FakePubRegistry&)            = delete;
    FakePubRegistry& operator=(const FakePubRegistry&) = delete;

    [[nodiscard]] bool allResolved() const noexcept { return !peers_.empty(); }

    int64_t sendToPeer(NodeId peer, const uint8_t* buf, std::size_t len) noexcept
    {
        offers_[peer].push_back({std::vector<uint8_t>(buf, buf + len)});
        return 1;  // simulate accepted
    }

    void broadcastToPeers(const uint8_t* buf, std::size_t len) noexcept
    {
        for (NodeId id : peers_) {
            sendToPeer(id, buf, len);
        }
    }

    // -----------------------------------------------------------------------
    // Test accessors
    // -----------------------------------------------------------------------
    [[nodiscard]] const std::vector<Offer>& offersTo(NodeId peer) const
    {
        static const std::vector<Offer> empty;
        auto it = offers_.find(peer);
        return (it != offers_.end()) ? it->second : empty;
    }

    [[nodiscard]] std::size_t totalOffersTo(NodeId peer) const
    {
        return offersTo(peer).size();
    }

    [[nodiscard]] const std::vector<NodeId>& peers() const noexcept { return peers_; }

    void clear() {
        for (auto& [id, v] : offers_) v.clear();
    }

private:
    std::vector<NodeId>                              peers_;
    std::unordered_map<NodeId, std::vector<Offer>>   offers_;
};

// ---------------------------------------------------------------------------
// Alias for a test-mode transport.
// ---------------------------------------------------------------------------
using TestTransport = WorkerReplicationTransportImpl<FakePubRegistry>;

// ---------------------------------------------------------------------------
// Factory: construct a TestTransport with workerId=1, peers={2, 3}.
// Returns the transport AND a reference to the embedded FakePubRegistry.
// Because FakePubRegistry is stored by value inside the template and we need
// to observe it after construction, we capture a pointer by accessing it from
// a wrapper that exposes the registry.
//
// Simpler: build the FakePubRegistry separately, move it into the transport,
// and expose what was recorded after.
// ---------------------------------------------------------------------------

// Helper to build a FetchResponseMsg with a records blob.
static FetchResponseMsg makeResp(PartitionId partition,
                                  std::vector<uint8_t> recordData = {})
{
    FetchResponseMsg resp{};
    resp.leaderEpoch    = 10;
    resp.partition      = partition;
    resp.highWatermark  = 100;
    resp.logStartOffset = 0;
    resp.errorCode      = 0;
    resp.topicId        = 0;
    // Store the data so the span is valid for the test's lifetime.
    // resp.records is a non-owning span — caller must keep data alive.
    // We return the raw msg; caller assigns records after storing the data.
    (void)recordData;
    return resp;
}

// Helper: encode expected FetchResponse bytes (for round-trip comparison).
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

// Helper: encode expected FetchRequest bytes.
static std::vector<uint8_t> encodeExpectedFetchRequest(const FetchRequestMsg& req)
{
    std::vector<uint8_t> buf(FetchRequestMsg::TOTAL_SIZE, 0);
    FetchRequestMsg::encode(buf.data(), 0,
                            req.leaderEpoch, req.partition, req.followerId,
                            req.fetchOffset, req.maxBytes, req.topicId);
    return buf;
}

// Helper: encode expected OLE req bytes.
static std::vector<uint8_t> encodeExpectedOleReq(const OffsetsForLeaderEpochReqMsg& req)
{
    std::vector<uint8_t> buf(OffsetsForLeaderEpochReqMsg::TOTAL_SIZE, 0);
    OffsetsForLeaderEpochReqMsg::encode(buf.data(), 0,
                                        req.currentEpoch, req.partition,
                                        req.followerId, req.requestedEpoch,
                                        req.followerLeo, req.topicId);
    return buf;
}

// Helper: encode expected OLE resp bytes.
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
// Test fixture
// ===========================================================================
class WorkerReplicationTransportTest : public ::testing::Test {
protected:
    // Peers: nodes 2 and 3.
    static constexpr NodeId kPeer2 = 2;
    static constexpr NodeId kPeer3 = 3;

    // Build a transport + extract a separate registry for observation.
    // Because the registry is moved into the transport, we build a second
    // identical registry for expected-state comparison.  Alternatively, we
    // expose the registry via a method.
    //
    // To avoid this complexity, we use a wrapper that holds both:
    struct Harness {
        // The second registry is used ONLY for verifying which peers exist.
        TestTransport transport;
        FakePubRegistry& reg;  // reference into transport's internal PubRegistry

        // We can't easily get a reference into the moved-from registry.
        // Instead: use a pointer trick by constructing with a factory that
        // returns both.
    };

    // Because we can't hold a reference into a moved PubRegistry easily,
    // we wrap via a helper that provides access via transport.pubReg().
    // Alternatively: expose pubReg_ as a public member in the test
    // template specialisation.
    //
    // SIMPLEST approach: build the transport, then call the send methods,
    // and observe via a shared pointer to the FakePubRegistry.
    //
    // Use std::shared_ptr<FakePubRegistry> so it outlives the transport.
    // But FakePubRegistry is stored by VALUE in the template; moving a
    // shared_ptr doesn't help.
    //
    // RESOLUTION: subclass TestTransport to expose the reg_ for testing.
    // Or: wrap FakePubRegistry in a ref-counted shell.  The cleanest approach
    // for a test is:
    //
    //   FakePubRegistry reg({2, 3});
    //   TestTransport t(workerId, std::move(reg));
    //   // After move, reg is in moved-from state.
    //
    // Instead, capture the results via a "proxy FakePubRegistry" that wraps
    // a shared captures vector:

    // FINAL approach: since the brief shows `pubs.offersTo(3)` being checked
    // AFTER `t.sendFetchResponse(3, resp)`, and pubs is separate from t,
    // we model this as: pubs is a wrapper whose internal state is shared.
    // We implement this via a SharedFakePubRegistry.
    void SetUp() override {}
};

// ===========================================================================
// SharedFakePubRegistry — wraps FakePubRegistry via shared_ptr so the
// observation handle (pubs) and the registry inside the transport share state.
// ===========================================================================
struct SharedState {
    std::vector<NodeId>                              peers;
    std::unordered_map<NodeId, std::vector<std::vector<uint8_t>>> offersTo;

    explicit SharedState(std::vector<NodeId> p) : peers(std::move(p)) {
        for (NodeId id : peers) offersTo[id];  // ensure entries exist
    }
};

class SharedFakePubRegistry {
public:
    explicit SharedFakePubRegistry(std::vector<NodeId> peers)
        : state_(std::make_shared<SharedState>(std::move(peers)))
    {}

    // Allow move construction so it can be passed to the transport ctor.
    SharedFakePubRegistry(SharedFakePubRegistry&&)            = default;
    SharedFakePubRegistry& operator=(SharedFakePubRegistry&&) = default;
    SharedFakePubRegistry(const SharedFakePubRegistry&)        = default;
    SharedFakePubRegistry& operator=(const SharedFakePubRegistry&) = default;

    [[nodiscard]] bool allResolved() const noexcept { return !state_->peers.empty(); }

    int64_t sendToPeer(NodeId peer, const uint8_t* buf, std::size_t len) noexcept
    {
        state_->offersTo[peer].emplace_back(buf, buf + len);
        return 1;
    }

    void broadcastToPeers(const uint8_t* buf, std::size_t len) noexcept
    {
        for (NodeId id : state_->peers) sendToPeer(id, buf, len);
    }

    // Observation interface (same as FakePubRegistry).
    [[nodiscard]] const std::vector<std::vector<uint8_t>>& offersTo(NodeId peer) const
    {
        static const std::vector<std::vector<uint8_t>> empty;
        auto it = state_->offersTo.find(peer);
        return (it != state_->offersTo.end()) ? it->second : empty;
    }

    [[nodiscard]] const std::vector<NodeId>& peers() const noexcept { return state_->peers; }

    void clear() { for (auto& [id, v] : state_->offersTo) v.clear(); }

private:
    std::shared_ptr<SharedState> state_;
};

using SharedTestTransport = WorkerReplicationTransportImpl<SharedFakePubRegistry>;

// Helper: make a SharedFakePubRegistry + SharedTestTransport sharing state.
// Returns pubs (for observation) and a heap-allocated transport (non-movable).
struct TestHarness {
    SharedFakePubRegistry          pubs;
    std::unique_ptr<SharedTestTransport> transport;
};

static TestHarness makeTestPair(int32_t workerId, std::vector<NodeId> peers)
{
    SharedFakePubRegistry pubs(peers);
    SharedFakePubRegistry pubsCopy = pubs;  // shared_ptr copy, same underlying state
    auto transport = std::make_unique<SharedTestTransport>(workerId, std::move(pubsCopy));
    return TestHarness{std::move(pubs), std::move(transport)};
}

// ===========================================================================
// 1. sendFetchResponse selects the correct peer pub and encodes the frame.
// ===========================================================================
TEST(WorkerReplicationTransport, SendFetchResponseSelectsPeerPubAndEncodesFrame)
{
    auto h = makeTestPair(/*workerId=*/1, {2, 3});

    // Build a resp with a non-empty records blob.
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

    // Exactly one offer to peer 3.
    ASSERT_EQ(h.pubs.offersTo(3).size(), 1u)
        << "sendFetchResponse should send exactly one frame to the target follower";
    // No offer to peer 2.
    EXPECT_TRUE(h.pubs.offersTo(2).empty())
        << "sendFetchResponse must NOT send to non-target peer";

    // Verify the frame bytes by round-trip decode.
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

    // The frame templateId must be FetchResponse (not reconcile).
    EXPECT_EQ(peekTemplateId(frame.data(), 0),
              static_cast<uint16_t>(FetchResponseMsg::TEMPLATE_ID));

    // The encoded bytes must be identical to the reference encoding.
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

    // Must be delivered to BOTH peers.
    ASSERT_EQ(h.pubs.offersTo(2).size(), 1u) << "sendFetchRequest must broadcast to peer 2";
    ASSERT_EQ(h.pubs.offersTo(3).size(), 1u) << "sendFetchRequest must broadcast to peer 3";

    // Verify encoding of peer 2's frame (peer 3 is identical by construction).
    const auto& frame2 = h.pubs.offersTo(2)[0];
    const FetchRequestMsg decoded = FetchRequestMsg::decode(frame2.data(), 0);
    EXPECT_EQ(decoded.leaderEpoch, req.leaderEpoch);
    EXPECT_EQ(decoded.partition,   req.partition);
    EXPECT_EQ(decoded.followerId,  req.followerId);
    EXPECT_EQ(decoded.fetchOffset, req.fetchOffset);
    EXPECT_EQ(decoded.maxBytes,    req.maxBytes);
    EXPECT_EQ(decoded.topicId,     req.topicId);

    // templateId = FetchRequest.
    EXPECT_EQ(peekTemplateId(frame2.data(), 0),
              static_cast<uint16_t>(FetchRequestMsg::TEMPLATE_ID));

    // Bytes identical to reference encoding.
    const auto expected = encodeExpectedFetchRequest(req);
    ASSERT_EQ(frame2.size(), expected.size());
    EXPECT_EQ(0, std::memcmp(frame2.data(), expected.data(), expected.size()));

    // Both peers received the same bytes.
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

    // templateId MUST be kReconcileFetchReqTemplateId, NOT FetchRequest::TEMPLATE_ID.
    EXPECT_EQ(peekTemplateId(frame.data(), 0), kReconcileFetchReqTemplateId)
        << "sendReconcileRequest must re-stamp the templateId to ReconcileFetchReq";
    EXPECT_NE(peekTemplateId(frame.data(), 0),
              static_cast<uint16_t>(FetchRequestMsg::TEMPLATE_ID));

    // Body decodes correctly (re-use FetchRequest decoder, the layout is identical).
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

    // templateId MUST be kReconcileFetchRespTemplateId.
    EXPECT_EQ(peekTemplateId(frame.data(), 0), kReconcileFetchRespTemplateId)
        << "sendReconcileResponse must re-stamp the templateId to ReconcileFetchResp";
    EXPECT_NE(peekTemplateId(frame.data(), 0),
              static_cast<uint16_t>(FetchResponseMsg::TEMPLATE_ID));

    // Body decodes correctly.
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
    // testMode_ = true, sub_ = null; resolved() delegates to pubReg_.allResolved()
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
    resp.records        = {};  // empty — HW-only update

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
