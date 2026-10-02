// ---------------------------------------------------------------------------
// F27SendNeverDroppedRegressionTest.cpp — a replication frame refused by the
// transport (Aeron BACK_PRESSURED -2 / ADMIN_ACTION -3 / NOT_CONNECTED -1) is
// re-sent on the NEXT pass, never recovered only by a resend timer.
//
// Root cause (F27): WorkerReplicationTransport discarded the Aeron position of
// every replication send; PartitionReplica::pushToFollowers advanced the
// follower's pushedThrough and the shared lastPushedOffset_ unconditionally,
// and decideFetchServe answered a fetch below pushedThrough AckOnly / Conflate
// until kFetchResendNs elapsed → the first batches after startup stayed
// unreplicated past the acks=all deadline (bench: 200000 attempted, 0 ok).
//
// Every test here runs on a FROZEN injected clock (kNow never advances): a
// recovery that needs time to pass cannot happen, so a green run proves the
// recovery is time-independent.  Real WorkerReplicationTransportImpl (the
// production send path) over a scripted PubRegistry test seam; no sleeps; every
// drive loop is capped and asserts.
// ---------------------------------------------------------------------------

#include <rune/node/WorkerReplicationTransport.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/FollowerSlots.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/InMemoryLog.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <deque>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

using namespace rune::replication;

namespace {

constexpr int64_t kNow = 1'000'000'000;   // FROZEN clock for every pass
constexpr int64_t kAeronNotConnected = -1;
constexpr int64_t kAeronBackPressured = -2;
constexpr int64_t kAeronAdminAction   = -3;

// ---------------------------------------------------------------------------
// Wire — the shared state behind a ScriptedPubRegistry (the transport owns a
// copy of the registry; the test keeps another; both see this state).
// ---------------------------------------------------------------------------
struct Wire {
    std::vector<NodeId> peers;
    std::unordered_map<NodeId, std::deque<std::vector<uint8_t>>> out;     // ACCEPTED frames per peer
    std::unordered_map<NodeId, std::deque<int64_t>>               script;  // next codes per peer (<=0 refuses)
    std::vector<NodeId> notConnected;                                      // NOT_CONNECTED until connect()
    int64_t     refuseEveryNthLarge = 0;   // 0 = off; else every Nth large data send is refused
    std::size_t largeBytes          = 4096;
    uint64_t    largeSeen           = 0;
    uint64_t    refused             = 0;
    int64_t     position            = 0;
};

class ScriptedPubRegistry {
public:
    explicit ScriptedPubRegistry(std::shared_ptr<Wire> w) : w_(std::move(w)) {}

    [[nodiscard]] bool    allResolved() const noexcept { return !w_->peers.empty(); }
    [[nodiscard]] bool    hasPeer(NodeId p) const noexcept { return isPeer_(p); }
    [[nodiscard]] int32_t maxMessageLength() const noexcept { return 16 << 20; }
    void setPeerBlocked(NodeId, bool) noexcept {}

    int64_t sendToPeer(NodeId peer, const uint8_t* buf, std::size_t len) noexcept {
        return offer_(peer, buf, len, nullptr, 0);
    }
    int64_t sendControlToPeer(NodeId peer, const uint8_t* buf, std::size_t len) noexcept {
        return offer_(peer, buf, len, nullptr, 0);
    }
    bool broadcastToPeers(const uint8_t* buf, std::size_t len) noexcept {
        bool all = !w_->peers.empty();
        for (NodeId p : w_->peers) if (offer_(p, buf, len, nullptr, 0) <= 0) all = false;
        return all;
    }
    int64_t sendGatherToPeer(NodeId peer, const uint8_t* prefix, std::size_t prefixLen,
                             std::span<const uint8_t> body) noexcept {
        return offer_(peer, prefix, prefixLen, body.data(), body.size());
    }

private:
    [[nodiscard]] bool isPeer_(NodeId p) const noexcept {
        return std::find(w_->peers.begin(), w_->peers.end(), p) != w_->peers.end();
    }
    int64_t offer_(NodeId peer, const uint8_t* a, std::size_t al, const uint8_t* b, std::size_t bl) {
        if (!isPeer_(peer)) return kAeronNotConnected;
        if (std::find(w_->notConnected.begin(), w_->notConnected.end(), peer) != w_->notConnected.end()) {
            ++w_->refused;
            return kAeronNotConnected;
        }
        auto& sc = w_->script[peer];
        if (!sc.empty()) {
            const int64_t code = sc.front();
            sc.pop_front();
            if (code <= 0) { ++w_->refused; return code; }
        }
        if (w_->refuseEveryNthLarge > 0 && al + bl >= w_->largeBytes &&
            (++w_->largeSeen % static_cast<uint64_t>(w_->refuseEveryNthLarge)) == 0) {
            ++w_->refused;
            return (w_->largeSeen & 1u) ? kAeronBackPressured : kAeronAdminAction;
        }
        std::vector<uint8_t> f(a, a + al);
        if (bl) f.insert(f.end(), b, b + bl);
        w_->out[peer].push_back(std::move(f));
        return ++w_->position;
    }
    std::shared_ptr<Wire> w_;
};

using ScriptedTransport = rune::replication::WorkerReplicationTransportImpl<ScriptedPubRegistry>;

PartitionConfig cfgFor(NodeId self) {
    PartitionConfig c;
    c.partitionId = 0; c.topicId = 0; c.selfId = self; c.minIsr = 3;
    c.replicaLagMaxMs = 60'000; c.replicaLagMaxOffset = 1'000'000;
    c.fetchIntervalNs = 1'000'000; c.isrCheckIntervalNs = 1'000'000'000;
    c.maxFetchBytes = 1 << 20; c.pushOnAppend = true;
    c.durability = DurabilityMode::AllInSync;   // acks=all: HW = min over the whole ISR
    return c;
}

std::vector<uint8_t> batchAt(Offset base, std::size_t valueBytes) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    b.append("k", std::string(valueBytes, static_cast<char>('a' + (base % 26))), 1000);
    return b.build();
}

struct Decoded { uint16_t templateId; FetchRequestMsg req; FetchResponseMsg resp; };

Decoded decode(const std::vector<uint8_t>& f) {
    Decoded d{};
    d.templateId = peekTemplateId(f.data(), 0);
    if (d.templateId == static_cast<uint16_t>(ReplicationTemplateId::FetchRequest))
        d.req = FetchRequestMsg::decode(f.data(), 0);
    else if (d.templateId == static_cast<uint16_t>(ReplicationTemplateId::FetchResponse))
        d.resp = FetchResponseMsg::decode(f.data(), 0, f.size());
    return d;
}

// One node: its in-memory log, its wire, its REAL per-worker transport, its replica.
struct Node {
    NodeId                            id;
    std::shared_ptr<Wire>             wire;
    rune::storage::InMemoryLog        log{8u << 20, 1u << 12};
    std::unique_ptr<ScriptedTransport> t;
    std::unique_ptr<PartitionReplica> r;

    Node(NodeId self, std::vector<NodeId> peers, Role role)
        : id(self), wire(std::make_shared<Wire>())
    {
        wire->peers = std::move(peers);
        t = std::make_unique<ScriptedTransport>(/*workerId=*/0, ScriptedPubRegistry{wire});
        r = std::make_unique<PartitionReplica>(cfgFor(self), role, /*epoch=*/1,
                                               std::vector<NodeId>{1, 2, 3},
                                               std::vector<NodeId>{1, 2, 3}, &log, t.get());
    }
    /// Accepted frames to `peer` with this SBE templateId.
    [[nodiscard]] std::size_t framesTo(NodeId peer, uint16_t templateId) const {
        std::size_t n = 0;
        auto it = wire->out.find(peer);
        if (it == wire->out.end()) return 0;
        for (const auto& f : it->second) if (peekTemplateId(f.data(), 0) == templateId) ++n;
        return n;
    }
    /// Accepted frames to `peer` that carry records (pushes / data fetch answers).
    [[nodiscard]] std::size_t dataFramesTo(NodeId peer) const {
        std::size_t n = 0;
        auto it = wire->out.find(peer);
        if (it == wire->out.end()) return 0;
        for (const auto& f : it->second) {
            const Decoded d = decode(f);
            if (d.templateId == static_cast<uint16_t>(ReplicationTemplateId::FetchResponse) &&
                !d.resp.records.empty()) ++n;
        }
        return n;
    }
};

// Three-node cluster (1 = leader) wired through the real transports.
struct Cluster {
    std::array<std::unique_ptr<Node>, 3> n{
        std::make_unique<Node>(1, std::vector<NodeId>{2, 3}, Role::Leader),
        std::make_unique<Node>(2, std::vector<NodeId>{1, 3}, Role::Follower),
        std::make_unique<Node>(3, std::vector<NodeId>{1, 2}, Role::Follower)};
    Offset nextOffset = 0;

    Cluster() {
        n[1]->r->becomeFollower(1, 1);
        n[2]->r->becomeFollower(1, 1);
    }
    Node& leader() { return *n[0]; }
    Node& node(NodeId id) { return *n[static_cast<std::size_t>(id - 1)]; }

    /// Produce hot path: append straight to the leader's log + onLeaderLogAppended.
    Offset produce(std::size_t valueBytes) {
        const Offset base = nextOffset++;
        leader().log.append(batchAt(base, valueBytes), base);
        leader().r->onLeaderLogAppended();
        return base;
    }

    /// Deliver every accepted frame (FIFO per sender→receiver), like Aeron images.
    int route() {
        int delivered = 0;
        for (auto& src : n) {
            for (auto& [peer, q] : src->wire->out) {
                while (!q.empty()) {
                    const std::vector<uint8_t> f = std::move(q.front());
                    q.pop_front();
                    const Decoded d = decode(f);
                    Node& dst = node(peer);
                    if (d.templateId == static_cast<uint16_t>(ReplicationTemplateId::FetchRequest)) {
                        if (dst.r->role() == Role::Leader) (void)dst.r->onFetchRequest(d.req, kNow);
                    } else if (d.templateId == static_cast<uint16_t>(ReplicationTemplateId::FetchResponse)) {
                        if (dst.r->role() == Role::Follower) dst.r->applyReplicated(d.resp);
                    }
                    ++delivered;
                }
            }
        }
        return delivered;
    }

    /// One pass: every node's doWork at the FROZEN clock, then route.
    void pass() {
        for (auto& x : n) (void)x->r->doWork(kNow);
        (void)route();
    }
};

} // namespace

// ---------------------------------------------------------------------------
// 1. A push back-pressured to follower 3 is re-sent on the very next doWork
//    pass (same nowNs) from 3's own cursor; follower 2 (which got it) is NOT
//    re-sent; the HW reaches the LEO once 3 acks.
// ---------------------------------------------------------------------------
TEST(F27SendNeverDroppedRegression, PushBackPressuredIsResentNextPassWithoutTimerRegression) {
    Cluster c;
    Node& L = c.leader();
    const Offset base = c.produce(64 * 1024);
    L.wire->script[3].push_back(kAeronBackPressured);   // the next send to 3 is refused

    (void)L.r->doWork(kNow);                            // pass 1: push
    EXPECT_LE(L.r->highWatermark(), base) << "acks=all: nothing committed before 3 has it";
    EXPECT_EQ(L.dataFramesTo(2), 1u) << "follower 2 got the push";
    EXPECT_EQ(L.dataFramesTo(3), 0u) << "the push to follower 3 was back-pressured";

    (void)L.r->doWork(kNow);                            // pass 2: SAME clock
    EXPECT_EQ(L.dataFramesTo(3), 1u)
        << "a back-pressured push must be re-sent on the next pass, not after a resend timer";
    EXPECT_EQ(L.dataFramesTo(2), 1u) << "follower 2 already has it: never re-sent";

    // Deliver + let both followers fast-ack: the HW covers the batch.
    for (int i = 0; i < 4; ++i) c.pass();
    EXPECT_GT(L.r->highWatermark(), base) << "acks=all commit point reached on a frozen clock";
}

// ---------------------------------------------------------------------------
// 2. A fetch answer that fails to send is not remembered as served: the next
//    identical fetch (same offset, same LEO, same nowNs) is answered at once —
//    no Conflate on an answer that never left.
// ---------------------------------------------------------------------------
TEST(F27SendNeverDroppedRegression, FetchResponseBackPressuredNextFetchServedImmediatelyRegression) {
    Cluster c;
    Node& L = c.leader();
    // Data reaches follower 2 by its fetch (no push yet: the produce is not
    // followed by a leader doWork before the fetch).
    (void)c.produce(1024);
    const FetchRequestMsg f{/*epoch=*/1, /*part=*/0, /*follower=*/2, /*off=*/0, /*max=*/1 << 20, /*topic=*/0};

    L.wire->script[2].push_back(kAeronAdminAction);
    (void)L.r->onFetchRequest(f, kNow);
    EXPECT_EQ(L.dataFramesTo(2), 0u) << "the answer was refused by the transport";

    (void)L.r->onFetchRequest(f, kNow);              // identical request, same clock
    EXPECT_EQ(L.dataFramesTo(2), 1u)
        << "an unsent answer must not conflate the retry — serve it immediately";
}

// ---------------------------------------------------------------------------
// 2b. A fetch BELOW a push that was refused is served Data at once (the
//     AckOnly gate only covers a push that was actually SENT).
// ---------------------------------------------------------------------------
TEST(F27SendNeverDroppedRegression, FetchBelowRefusedPushIsServedDataRegression) {
    Cluster c;
    Node& L = c.leader();
    (void)c.produce(1024);
    L.wire->script[3].push_back(kAeronBackPressured);   // the push to 3 is refused
    (void)L.r->doWork(kNow);
    ASSERT_EQ(L.dataFramesTo(3), 0u);

    const FetchRequestMsg f{1, 0, /*follower=*/3, /*off=*/0, 1 << 20, 0};
    (void)L.r->onFetchRequest(f, kNow);
    EXPECT_EQ(L.dataFramesTo(3), 1u)
        << "nothing was SENT to follower 3: its fetch must be served Data, not AckOnly";
}

// ---------------------------------------------------------------------------
// 3. acks=all under intermittent back-pressure: every 3rd large send (push,
//    fetch answer) is refused (BACK_PRESSURED / ADMIN_ACTION alternately).
//    Every append must commit (HW past it) within a capped number of passes on
//    a FROZEN clock — none may wait for a timer (= the acks=all timeout).
// ---------------------------------------------------------------------------
TEST(F27SendNeverDroppedRegression, AcksAllCompletesUnderIntermittentBackPressureRegression) {
    Cluster c;
    for (auto& x : c.n) { x->wire->refuseEveryNthLarge = 3; x->wire->largeBytes = 16 * 1024; }

    constexpr int kAppends = 60;
    constexpr int kPassCap = 16;
    int committed = 0;
    for (int i = 0; i < kAppends; ++i) {
        const Offset base = c.produce(32 * 1024);
        bool done = false;
        for (int p = 0; p < kPassCap && !done; ++p) {
            c.pass();
            done = c.leader().r->highWatermark() > base;
        }
        ASSERT_TRUE(done) << "append " << i << " (offset " << base
                          << ") did not commit on a frozen clock — a refused send waited for a timer"
                          << " (hw=" << c.leader().r->highWatermark() << ")";
        ++committed;
    }
    EXPECT_EQ(committed, kAppends);
    uint64_t refused = 0;
    for (auto& x : c.n) refused += x->wire->refused;
    EXPECT_GT(refused, 10u) << "precondition: the transport really refused sends";
    EXPECT_EQ(c.node(2).r->logEndOffset(), c.leader().r->logEndOffset());
    EXPECT_EQ(c.node(3).r->logEndOffset(), c.leader().r->logEndOffset());
}

// ---------------------------------------------------------------------------
// 4. Startup: the peer image is not connected yet (tryClaim NOT_CONNECTED).
//    Once it connects, the push is re-sent on the next pass — no timer.
// ---------------------------------------------------------------------------
TEST(F27SendNeverDroppedRegression, StartupNotConnectedPushRetriedOnceImageConnectsRegression) {
    Cluster c;
    Node& L = c.leader();
    L.wire->notConnected = {2, 3};
    const Offset base = c.produce(64 * 1024);
    for (int i = 0; i < 3; ++i) (void)L.r->doWork(kNow);
    ASSERT_EQ(L.dataFramesTo(2) + L.dataFramesTo(3), 0u);

    L.wire->notConnected.clear();                     // images connect
    (void)L.r->doWork(kNow);
    EXPECT_EQ(L.dataFramesTo(2), 1u) << "push re-sent to 2 once its image connected";
    EXPECT_EQ(L.dataFramesTo(3), 1u) << "push re-sent to 3 once its image connected";
    for (int i = 0; i < 4; ++i) c.pass();
    EXPECT_GT(L.r->highWatermark(), base);
}

// ---------------------------------------------------------------------------
// 5. Follower side: a fetch request / fast-ack refused by the transport is
//    re-sent on the next pass (same clock), not after the fetch resend window.
// ---------------------------------------------------------------------------
TEST(F27SendNeverDroppedRegression, FollowerFetchRefusedIsResentNextPassRegression) {
    Cluster c;
    Node& F = c.node(2);
    F.wire->script[1].push_back(kAeronBackPressured);
    (void)F.r->doWork(kNow);                          // bootstrap fetch → refused
    ASSERT_TRUE(F.wire->out[1].empty());
    (void)F.r->doWork(kNow);                          // same clock
    ASSERT_EQ(F.wire->out[1].size(), 1u) << "refused fetch must be re-sent next pass";
    const Decoded d = decode(F.wire->out[1].front());
    EXPECT_EQ(d.templateId, static_cast<uint16_t>(ReplicationTemplateId::FetchRequest));
}

TEST(F27SendNeverDroppedRegression, FollowerFastAckRefusedIsResentNextPassRegression) {
    Cluster c;
    Node& L = c.leader();
    Node& F = c.node(2);
    for (int i = 0; i < 3; ++i) c.pass();             // bootstrap fetches at kNow → InSync
    const Offset base = c.produce(1024);
    (void)L.r->doWork(kNow);                          // push to 2 and 3
    F.wire->script[1].push_back(kAeronBackPressured); // 2's fast-ack is refused
    (void)c.route();                                  // 2 + 3 apply; 2's ack refused
    (void)c.route();                                  // 3's ack reaches the leader
    (void)L.r->doWork(kNow);
    ASSERT_LE(L.r->highWatermark(), base) << "precondition: 2's ack never left";
    (void)F.r->doWork(kNow);                          // SAME clock → ack re-sent
    (void)c.route();
    (void)L.r->doWork(kNow);
    EXPECT_GT(L.r->highWatermark(), base)
        << "a refused fast-ack must be re-sent on the next pass, not after a fetch timer";
}

// ---------------------------------------------------------------------------
// 6. The leader re-sends a REFUSED fetch answer on its own next pass — the
//    follower does not have to wait out its fetch resend window to re-ask.
// ---------------------------------------------------------------------------
TEST(F27SendNeverDroppedRegression, RefusedFetchAnswerIsResentByLeaderNextPassRegression) {
    // (a) HW-only answer (follower 2 is at the LEO).
    {
        Cluster c;
        Node& L = c.leader();
        (void)c.produce(1024);
        (void)L.r->doWork(kNow);                              // push accepted by 2 and 3
        ASSERT_EQ(L.wire->out[2].size(), 1u);
        L.wire->script[2].push_back(kAeronBackPressured);
        (void)L.r->onFetchRequest(FetchRequestMsg{1, 0, 2, /*off=*/1, 1 << 20, 0}, kNow);
        ASSERT_EQ(L.wire->out[2].size(), 1u) << "the answer was refused";
        (void)L.r->doWork(kNow);                              // SAME clock, no new fetch
        EXPECT_EQ(L.wire->out[2].size(), 2u) << "the owed HW answer must be re-sent next pass";
        (void)L.r->doWork(kNow);
        EXPECT_EQ(L.wire->out[2].size(), 2u) << "and exactly once";
    }
    // (b) Data answer.
    {
        Cluster c;
        Node& L = c.leader();
        (void)c.produce(1024);
        L.wire->script[2].push_back(kAeronAdminAction);
        (void)L.r->onFetchRequest(FetchRequestMsg{1, 0, 2, /*off=*/0, 1 << 20, 0}, kNow);
        ASSERT_EQ(L.dataFramesTo(2), 0u);
        ASSERT_EQ(L.r->fetchDataServesForTest(), 0u);
        (void)L.r->doWork(kNow);
        EXPECT_EQ(L.r->fetchDataServesForTest(), 1u) << "the owed Data answer must be re-sent next pass";
        EXPECT_GE(L.dataFramesTo(2), 1u);
        EXPECT_GE(L.r->sendRefusalsForTest(), 1u);
    }
}

// ---------------------------------------------------------------------------
// 7. Transport API: EVERY replication send reports refusal (NOT_CONNECTED /
//    BACK_PRESSURED / ADMIN_ACTION → false) and acceptance (position > 0 → true).
// ---------------------------------------------------------------------------
TEST(F27SendNeverDroppedRegression, TransportReportsRefusalForEverySendRegression) {
    auto w = std::make_shared<Wire>();
    w->peers = {2, 3};
    ScriptedTransport t(/*workerId=*/0, ScriptedPubRegistry{w});
    const FetchRequestMsg req{1, 0, 1, 0, 1024, 0};
    const std::vector<uint8_t> records(64 * 1024, 0x5A);
    FetchResponseMsg resp{};
    resp.leaderEpoch = 1; resp.records = std::span<const uint8_t>(records);
    const OffsetsForLeaderEpochReqMsg oreq{};
    const OffsetsForLeaderEpochRespMsg oresp{};

    PartitionTransport& base = t;   // the replica-facing API
    for (int64_t code : {kAeronNotConnected, kAeronBackPressured, kAeronAdminAction}) {
        SCOPED_TRACE(code);
        auto refuseNext = [&] { w->script[2].push_back(code); };
        refuseNext(); EXPECT_FALSE(base.sendFetchResponse(2, resp));
        refuseNext(); EXPECT_FALSE(base.sendReconcileResponse(2, resp));
        refuseNext(); EXPECT_FALSE(base.sendFetchRequestTo(2, req));
        refuseNext(); EXPECT_FALSE(base.sendReconcileRequest(2, req));
        refuseNext(); EXPECT_FALSE(base.sendOffsetsForLeaderEpochRequest(2, oreq));
        refuseNext(); EXPECT_FALSE(base.sendOffsetsForLeaderEpochResponse(2, oresp));
        refuseNext(); EXPECT_FALSE(base.sendFetchRequest(req)) << "broadcast: one peer refused";
    }
    EXPECT_TRUE(base.sendFetchResponse(2, resp));
    EXPECT_TRUE(base.sendReconcileResponse(3, resp));
    EXPECT_TRUE(base.sendFetchRequestTo(2, req));
    EXPECT_TRUE(base.sendReconcileRequest(2, req));
    EXPECT_TRUE(base.sendOffsetsForLeaderEpochRequest(2, oreq));
    EXPECT_TRUE(base.sendOffsetsForLeaderEpochResponse(2, oresp));
    EXPECT_TRUE(base.sendFetchRequest(req));
    EXPECT_FALSE(base.sendFetchResponse(/*unknown peer=*/9, resp)) << "no publication → refused";
}

// ===========================================================================
// Fix round 1 (review f27-review.md)
// ===========================================================================

std::vector<uint8_t> multiBatchAt(Offset base, int records) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    for (int i = 0; i < records; ++i) b.append("k", "v", 1000);
    return b.build();
}

// F2: an owed Data answer survives a pure ack at the SAME offset (a non-ISR,
// catching-up follower has no push to cover it).
TEST(F27SendNeverDroppedRegression, OwedDataAnswerSurvivesSameOffsetPureAckRegression) {
    Cluster c;
    Node& L = c.leader();
    L.r->becomeLeader(1, {1, 2});                      // follower 3 is NOT in the ISR
    (void)c.produce(1024);
    L.wire->script[3].push_back(kAeronBackPressured);
    (void)L.r->onFetchRequest(FetchRequestMsg{1, 0, 3, /*off=*/0, 1 << 20, 0}, kNow);   // Data → refused → owed
    (void)L.r->onFetchRequest(FetchRequestMsg{1, 0, 3, /*off=*/0, /*max=*/0, 0}, kNow); // pure ack, same offset
    ASSERT_EQ(L.dataFramesTo(3), 0u);
    (void)L.r->doWork(kNow);
    EXPECT_EQ(L.dataFramesTo(3), 1u)
        << "a same-offset pure ack must not discharge the owed Data answer";
}

// F3: a refused OffsetsForLeaderEpoch response is re-sent on the leader's next pass.
TEST(F27SendNeverDroppedRegression, RefusedEpochResponseIsResentNextPassRegression) {
    Cluster c;
    Node& L = c.leader();
    const uint16_t kEpochResp = static_cast<uint16_t>(ReplicationTemplateId::OffsetsForLeaderEpochResp);
    L.wire->script[2].push_back(kAeronBackPressured);
    OffsetsForLeaderEpochReqMsg req{};
    req.currentEpoch = 1; req.partition = 0; req.followerId = 2;
    req.requestedEpoch = 1; req.followerLeo = 0; req.topicId = 0;
    L.r->onOffsetsForLeaderEpochRequest(req);
    ASSERT_EQ(L.framesTo(2, kEpochResp), 0u);
    (void)L.r->doWork(kNow);
    EXPECT_EQ(L.framesTo(2, kEpochResp), 1u) << "refused epoch answer re-sent next pass";
    (void)L.r->doWork(kNow);
    EXPECT_EQ(L.framesTo(2, kEpochResp), 1u) << "exactly once";
}

// F3: a refused reverse-reconcile PULL (deficient leader → ahead survivor) is
// re-sent on the next pass (no re-probe / stuck-window wait).
TEST(F27SendNeverDroppedRegression, RefusedReconcilePullIsResentNextPassRegression) {
    Cluster c;
    Node& L = c.leader();
    L.r->becomeLeader(2, {1, 2, 3});                   // open epoch 2 at LEO 0
    L.wire->script[2].push_back(kAeronBackPressured);  // the pull is refused
    OffsetsForLeaderEpochReqMsg req{};
    req.currentEpoch = 2; req.partition = 0; req.followerId = 2;
    req.requestedEpoch = 2; req.followerLeo = 5; req.topicId = 0;   // ahead, shared epoch
    L.r->onOffsetsForLeaderEpochRequest(req);
    ASSERT_EQ(L.framesTo(2, kReconcileFetchReqTemplateId), 0u);
    (void)L.r->doWork(kNow);
    EXPECT_EQ(L.framesTo(2, kReconcileFetchReqTemplateId), 1u) << "refused pull re-sent next pass";
}

// F3: a refused survivor ReconcileResponse is re-sent on the survivor's next pass.
TEST(F27SendNeverDroppedRegression, RefusedReconcileResponseIsResentNextPassRegression) {
    Cluster c;
    Node& F = c.node(2);
    F.log.append(batchAt(0, 64), 0);                   // the survivor holds offset 0
    F.wire->script[1].push_back(kAeronAdminAction);
    F.r->onReconcileRequest(FetchRequestMsg{1, 0, /*puller=*/1, /*off=*/0, 1 << 20, 0});
    ASSERT_EQ(F.framesTo(1, kReconcileFetchRespTemplateId), 0u);
    (void)F.r->doWork(kNow);
    EXPECT_EQ(F.framesTo(1, kReconcileFetchRespTemplateId), 1u) << "refused reconcile answer re-sent";
}

// F4: a follower whose sends are all refused (leader image not connected) does
// not report work — the idle strategy must be able to back off.
TEST(F27SendNeverDroppedRegression, RefusedFollowerSendsAreNotWorkRegression) {
    Cluster c;
    Node& F = c.node(2);
    F.wire->notConnected = {1, 3};
    for (int i = 0; i < 3; ++i) EXPECT_EQ(F.r->doWork(kNow), 0) << "pass " << i;
    F.wire->notConnected.clear();
    EXPECT_GT(F.r->doWork(kNow), 0) << "the accepted re-send is work";
}

// F6: a Data range already SERVED (accepted) to a follower by its fetch is not
// pushed to it again.
TEST(F27SendNeverDroppedRegression, DataServedRangeIsNotRePushedRegression) {
    Cluster c;
    Node& L = c.leader();
    (void)c.produce(1024);
    (void)L.r->onFetchRequest(FetchRequestMsg{1, 0, 2, 0, 1 << 20, 0}, kNow);   // Data → accepted
    ASSERT_EQ(L.dataFramesTo(2), 1u);
    (void)L.r->doWork(kNow);
    EXPECT_EQ(L.dataFramesTo(2), 1u) << "the served range is not pushed again";
    EXPECT_EQ(L.dataFramesTo(3), 1u) << "3 still gets its push";
}

// F7: an owed Data answer to a dead follower is rebuilt (log walk) once, not
// every pass, while nothing changed.
TEST(F27SendNeverDroppedRegression, OwedAnswerToDeadFollowerIsNotRebuiltEveryPassRegression) {
    Cluster c;
    Node& L = c.leader();
    (void)c.produce(1024);
    L.wire->notConnected = {3};
    (void)L.r->onFetchRequest(FetchRequestMsg{1, 0, 3, 0, 1 << 20, 0}, kNow);   // refused → owed
    for (int i = 0; i < 20; ++i) (void)L.r->doWork(kNow);
    EXPECT_LE(L.r->owedAnswerRebuildsForTest(), 1u);
    L.wire->notConnected.clear();
    (void)L.r->doWork(kNow);
    EXPECT_GE(L.dataFramesTo(3), 1u) << "and it is still delivered once the image connects";
}

// F8: HW holds while one follower's pushes are refused; it advances once they land.
TEST(F27SendNeverDroppedRegression, HwHoldsWhilePushToFollowerIsRefusedRegression) {
    Cluster c;
    Node& L = c.leader();
    L.wire->notConnected = {3};
    const Offset base = c.produce(4096);
    for (int i = 0; i < 6; ++i) c.pass();
    EXPECT_GT(c.node(2).r->logEndOffset(), base) << "follower 2 has it";
    EXPECT_LE(L.r->highWatermark(), base) << "acks=all HW never counts the refused follower";
    L.wire->notConnected.clear();
    bool done = false;
    for (int i = 0; i < 8 && !done; ++i) { c.pass(); done = L.r->highWatermark() > base; }
    EXPECT_TRUE(done);
}

// F8: retries after refusals never push past the flow window
// (minFollowerLeo + kPushFlowWindow).
TEST(F27SendNeverDroppedRegression, FlowWindowHoldsUnderRefusalRetryRegression) {
    Cluster c;
    Node& L = c.leader();
    L.wire->notConnected = {3};                        // 3 never acks: ceiling = 0 + window
    constexpr int kPerBatch = 300;
    constexpr int kBatches  = 70;                      // 21000 offsets > window (16384)
    Offset next = 0;
    for (int b = 0; b < kBatches; ++b) {
        L.log.append(multiBatchAt(next, kPerBatch), next + kPerBatch - 1);
        next += kPerBatch;
    }
    c.nextOffset = next;
    L.r->onLeaderLogAppended();
    const Offset ceiling = PartitionReplica::kPushFlowWindowForTest;
    Offset maxBase = -1;
    for (int pass = 0; pass < 60; ++pass) {
        if (pass % 2 == 0) L.wire->script[2].push_back(kAeronBackPressured);   // intermittent refusals
        // Only the LEADER runs (followers apply pushes + fast-ack via route()):
        // every data frame to 2 is then a PUSH (fetch answers are not windowed).
        (void)L.r->doWork(kNow);
        for (const auto& f : L.wire->out[2]) {
            const Decoded d = decode(f);
            if (d.templateId != static_cast<uint16_t>(ReplicationTemplateId::FetchResponse)) continue;
            std::size_t pos = 0;
            while (pos + 61 <= d.resp.records.size()) {
                rune::protocol::RecordBatchReader r(d.resp.records.data() + pos, d.resp.records.size() - pos);
                if (r.baseOffset() > maxBase) maxBase = r.baseOffset();
                pos += static_cast<std::size_t>(r.totalLength());
            }
        }
        (void)c.route();
    }
    EXPECT_LT(maxBase, ceiling) << "no batch starting at/after minFollowerLeo + window is ever pushed";
    EXPECT_GE(c.node(2).r->logEndOffset(), ceiling - kPerBatch) << "follower 2 filled the window";
}

// ===========================================================================
// Fix round 2 (re-review f27-rereview-1.md)
// ===========================================================================

// R1: the legacy transport does NOT hold frames for a DOWN follower; the push it
// "accepted" meanwhile may be gone.  When the follower's connection generation
// changes (down→up), its next fetch below the old pushedThrough is served Data
// at once — no AckOnly, no resend window (frozen clock).
TEST(F27SendNeverDroppedRegression, ConnectionGenerationBumpRewindsFollowerPushStateRegression) {
    Cluster c;
    Node& L = c.leader();
    (void)c.produce(1024);
    (void)L.r->doWork(kNow);                                   // push accepted to 2 (pushedThrough = 1)
    ASSERT_EQ(L.dataFramesTo(2), 1u);
    const FetchRequestMsg f{1, 0, 2, /*off=*/0, 1 << 20, 0};
    (void)L.r->onFetchRequest(f, kNow, /*connGen=*/1);         // first sighting: covered by the push
    EXPECT_EQ(L.dataFramesTo(2), 1u) << "same link: the push covers it (AckOnly)";
    (void)L.r->onFetchRequest(f, kNow, /*connGen=*/2);         // the link went down and came back
    EXPECT_EQ(L.dataFramesTo(2), 2u)
        << "after a reconnect the 'pushed' range may be lost: serve Data immediately";
    (void)L.r->onFetchRequest(f, kNow, /*connGen=*/2);         // same generation again
    EXPECT_EQ(L.dataFramesTo(2), 2u) << "no further rewind without a new generation (conflated)";
}
