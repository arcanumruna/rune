// Checked writes Part 11, Task 9: routing TxnVote through the gateway.  A real WorkerPool and worker behind a real
// ClientGateway.  The local tests need no cluster plane and no Aeron driver; the remote ones feed the captured frames
// to aeron:ipc transports through deliverFrameForTest.
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/ReplicationMessages.h>
#include <gtest/gtest.h>
#include <Aeron.h>
#include <Context.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <vector>
#include <unistd.h>

using namespace rune::node;
using rune::gateway::TxnVoteResult;
using rune::replication::NodeId;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;
namespace fs = std::filesystem;

namespace {
struct VoteTmpDir {
    fs::path path;
    VoteTmpDir() {
        static std::atomic<int> seq{0};
        const auto t = std::chrono::steady_clock::now().time_since_epoch().count();
        path = fs::temp_directory_path() / ("rune_vote_" + std::to_string(::getpid()) + "_" + std::to_string(seq++) + "_" +
                                            std::to_string(t));
        fs::create_directories(path);
    }
    ~VoteTmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};
class VoteNullTransport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};

// One node: topic 0 with 1 partition, its leader replica on worker 0 of a real WorkerPool behind a real gateway.
struct VoteRig {
    struct Got { int32_t topicId; int32_t partition; int64_t producerId; int16_t producerEpoch; int32_t coordinatorEpoch; TxnVoteResult r; };
    VoteTmpDir d;
    rune::gateway::TopicRegistry topics{{rune::node::TopicSpec{"t", 1}}, d.path.string()};
    VoteNullTransport t;
    ReplicaRegistry reg;
    WorkerPool pool{1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16};
    rune::gateway::ClientGateway gw = rune::gateway::ClientGateway::makeForTest(
        [](const uint8_t*, std::size_t) -> int64_t { return 1; });
    std::vector<Got> got;
    int voteReturns = 0;   ///< TxnVoteResult returns the worker produced
    bool resendInSink = false;
    std::vector<int64_t> reentrantTokens;
    std::size_t inFlightAtSink = 99;
    PartitionReplica* replica = nullptr;
    // The cluster-plane channel: every frame the gateway sends is captured encoded, with its target node.
    std::vector<std::pair<int32_t, std::vector<uint8_t>>> votesSent, responsesSent;
    bool acceptVotes = true, acceptResponses = true;
    template <class M> static bool capture(std::vector<std::pair<int32_t, std::vector<uint8_t>>>& out, int32_t to, const M& m) {
        std::vector<uint8_t> f(M::TOTAL_SIZE);
        m.encode(f.data(), f.size());
        out.emplace_back(to, std::move(f));
        return true;
    }
    static bool sendMarkerStub(void*, int32_t, const rune::replication::WriteTxnMarkerMsg&) { return false; }
    static bool sendVote(void* self, int32_t to, const rune::replication::TxnVoteMsg& m) {
        auto* rig = static_cast<VoteRig*>(self);
        return rig->acceptVotes && capture(rig->votesSent, to, m);
    }
    static bool sendVoteResponse(void* self, int32_t to, const rune::replication::TxnVoteResponseMsg& m) {
        auto* rig = static_cast<VoteRig*>(self);
        return rig->acceptResponses && capture(rig->responsesSent, to, m);
    }
    explicit VoteRig(int32_t self = 0) {
        PartitionConfig c; c.topicId = 0; c.partitionId = 0; c.selfId = self; c.minIsr = 1; c.durabilityTier = 1;
        c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL; c.pushOnAppend = false;
        c.replicaLagMaxMs = 1'000'000;
        auto rep = std::make_unique<PartitionReplica>(c, Role::Leader, 1, std::vector<NodeId>{static_cast<NodeId>(self)},
                                                      std::vector<NodeId>{static_cast<NodeId>(self)}, topics.log(0, 0), &t);
        replica = rep.get();
        reg.add(0, 0, std::move(rep));
        pool.assignFromRegistry(reg);
        pool.setRegistry(&reg);
        pool.setRunnersStartedForTest(true);
        gw.setNodeIdForTest(self);
        gw.setTxnMarkerChannel({&VoteRig::sendMarkerStub, nullptr, this, nullptr, nullptr, &VoteRig::sendVote,
                                &VoteRig::sendVoteResponse});
        gw.setTopicRegistry(&topics);
        gw.setReplicaRegistry(&reg);
        gw.setWorkerPool(&pool);
        gw.setTxnVoteSink(&VoteRig::sink, this);
        runWorker(1);   // ownership applied
    }
    static void sink(void* self, int32_t topicId, int32_t partition, int64_t producerId, int16_t producerEpoch,
                     int32_t coordinatorEpoch, const TxnVoteResult& r) noexcept {
        auto* rig = static_cast<VoteRig*>(self);
        rig->got.push_back({topicId, partition, producerId, producerEpoch, coordinatorEpoch, r});
        rig->inFlightAtSink = rig->gw.txnVotesInFlightForTest();
        if (rig->resendInSink) {   // several sends from inside the drain; the empty-table check above is what catches a late erase
            rig->resendInSink = false;
            for (int i = 0; i < 8; ++i)
                rig->reentrantTokens.push_back(rig->gw.sendTxnVote(topicId, partition, producerId + 100, producerEpoch, coordinatorEpoch));
        }
    }
    void runWorker(int64_t now) {
        pool.worker(0).doWork(now);
        pool.worker(0).drainReturn([&](WorkerReturn ret) {
            if (ret.type == WorkerReturn::Type::TxnVoteResult) ++voteReturns;
            gw.handleWorkerReturnForTest(ret);
        });
    }
};

std::shared_ptr<const rune::metadata::RoutingImage> voteImage(int32_t leader, int32_t epoch) {
    std::vector<rune::metadata::RoutingImage::BuildEntry> e;
    for (int p = 0; p < 2; ++p) e.push_back({rune::metadata::gpidOf(0, p), leader, epoch});
    return rune::metadata::RoutingImage::build(e);
}
} // namespace

// The vote goes onto the owning worker's op ring and its TxnVoteResult comes back as one sink call, with the worker's fields.
TEST(CheckedWritesTxnVoteRouting, LocalVoteReachesTheWorkerAndAnswers) {
    VoteRig R;
    const int64_t token = R.gw.sendTxnVote(0, 0, 77, 2, 9);
    ASSERT_GE(token, 0);
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 1u);
    EXPECT_TRUE(R.got.empty()) << "never answered synchronously";
    R.runWorker(2);   // a plain partition judges nothing: yes
    ASSERT_EQ(R.got.size(), 1u);
    EXPECT_EQ(R.got[0].topicId, 0);
    EXPECT_EQ(R.got[0].partition, 0);
    EXPECT_EQ(R.got[0].producerId, 77);
    EXPECT_EQ(R.got[0].producerEpoch, 2);
    EXPECT_EQ(R.got[0].coordinatorEpoch, 9);
    EXPECT_EQ(R.got[0].r.errorCode, 0);
    EXPECT_EQ(R.got[0].r.code, 0u);
    EXPECT_EQ(R.got[0].r.constraintId, 0u);
    EXPECT_EQ(R.got[0].r.failingOffset, -1);
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 0u);

    // A partition with no registered replica: the worker answers NotLeader, which must not read as a yes vote.
    ASSERT_GE(R.gw.sendTxnVote(0, 1, 79, 1, 9), 0);
    R.runWorker(3);
    ASSERT_EQ(R.got.size(), 2u);
    EXPECT_EQ(R.got[1].partition, 1);
    EXPECT_EQ(R.got[1].r.errorCode, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader));
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 0u);

    // A no vote: the worker's fields map onto the result.  The op stays on the ring; the answer is injected.
    ASSERT_GE(R.gw.sendTxnVote(0, 0, 78, 1, 9), 0);
    WorkerReturn ret;
    ret.type = WorkerReturn::Type::TxnVoteResult;
    ret.correlationId = R.gw.lastVoteTokenForTest();
    ret.sessionId = -1;
    ret.errorCode = 0;
    ret.recordCount = 55;
    ret.newNextOffset = 0x00070000;
    ret.baseOffset = 123;
    R.gw.handleWorkerReturnForTest(ret);
    ASSERT_EQ(R.got.size(), 3u);
    EXPECT_EQ(R.got[2].producerId, 78);
    EXPECT_EQ(R.got[2].r.errorCode, 0);
    EXPECT_EQ(R.got[2].r.code, 55u);
    EXPECT_EQ(R.got[2].r.constraintId, 0x00070000u);
    EXPECT_EQ(R.got[2].r.failingOffset, 123);
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 0u);

    // A duplicate answer matches no token: dropped.
    R.gw.handleWorkerReturnForTest(ret);
    EXPECT_EQ(R.got.size(), 3u);
}

// The sink may send again from inside the drain: the answered entry is gone before it runs.
TEST(CheckedWritesTxnVoteRouting, SinkMaySendAgainReentrantly) {
    VoteRig R;
    R.resendInSink = true;
    ASSERT_GE(R.gw.sendTxnVote(0, 0, 77, 2, 9), 0);
    R.runWorker(2);
    ASSERT_EQ(R.got.size(), 1u);
    EXPECT_EQ(R.inFlightAtSink, 0u) << "the answered entry is erased before the sink runs";
    ASSERT_EQ(R.reentrantTokens.size(), 8u);
    for (const int64_t t : R.reentrantTokens) EXPECT_GE(t, 0);
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 8u);
    R.runWorker(3);
    ASSERT_EQ(R.got.size(), 9u);
    EXPECT_EQ(R.got[1].producerId, 177);
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 0u);
}

// A refused send returns -1, leaves no entry behind and is never answered.
TEST(CheckedWritesTxnVoteRouting, RefusedVoteReturnsMinusOneAndIsNeverAnswered) {
    {   // no worker pool
        VoteRig R;
        auto gw = rune::gateway::ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; });
        gw.setTxnVoteSink(&VoteRig::sink, &R);
        EXPECT_EQ(gw.sendTxnVote(0, 0, 77, 1, 9), -1);
        EXPECT_EQ(gw.txnVotesInFlightForTest(), 0u);
        EXPECT_TRUE(R.got.empty());
    }
    VoteRig R;   // full op ring
    std::size_t sent = 0;
    while (sent < rune::gateway::ClientGateway::kMaxTxnVotesInFlight &&
           R.gw.sendTxnVote(0, 0, 77 + static_cast<int64_t>(sent), 1, 9) >= 0) ++sent;
    ASSERT_LT(sent, rune::gateway::ClientGateway::kMaxTxnVotesInFlight) << "the ring never filled";
    ASSERT_GT(sent, 0u);
    const int64_t lastToken = R.gw.lastVoteTokenForTest();
    EXPECT_EQ(R.gw.sendTxnVote(0, 0, 1, 1, 9), -1);
    EXPECT_EQ(R.gw.lastVoteTokenForTest(), lastToken) << "a refused send does not draw a token";
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), sent);
    for (int pass = 0; pass < 1000 && R.gw.txnVotesInFlightForTest() > 0; ++pass) R.runWorker(2 + pass);
    EXPECT_EQ(R.got.size(), sent);
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 0u);
}

// Node 0 coordinates and node 1 leads: the vote crosses the plane as a TxnVoteMsg, the worker's answer comes back as a
// TxnVoteResponseMsg with the worker's fields, and each end is fed through a real transport's handleFrame_.
TEST(CheckedWritesTxnVoteRouting, RemoteVoteRoundTripsOverThePlane) {
    VoteTmpDir ad;
    rune::node::EmbeddedDriverConfig dc; dc.dir = ad.path.string(); dc.termBufferBytes = 64 * 1024;
    rune::node::EmbeddedDriver drv(dc, rune::node::DriverMode::Threaded);
    aeron::Context ctx; ctx.aeronDir(ad.path.string());
    auto client = aeron::Aeron::connect(ctx);
    rune::replication::AeronPartitionTransport tr0(client, "aeron:ipc", 7401, 0, 1 << 20);
    rune::replication::AeronPartitionTransport tr1(client, "aeron:ipc", 7402, 1, 1 << 20);
    VoteRig C(0), L(1);
    tr0.setTxnVoteHandlers(&rune::gateway::ClientGateway::onTxnVoteThunk,
                           &rune::gateway::ClientGateway::onTxnVoteResponseThunk, &C.gw);
    tr1.setTxnVoteHandlers(&rune::gateway::ClientGateway::onTxnVoteThunk,
                           &rune::gateway::ClientGateway::onTxnVoteResponseThunk, &L.gw);
    C.gw.setRoutingImageSnapshot(voteImage(1, 1));
    auto ferry = [](auto& out, rune::replication::AeronPartitionTransport& to, int32_t expectTo) {
        ASSERT_FALSE(out.empty());
        EXPECT_EQ(out.back().first, expectTo);
        to.deliverFrameForTest(out.back().second.data(), out.back().second.size(), 1);
        out.clear();
    };

    // A yes vote from a plain partition.
    const int64_t token = C.gw.sendTxnVote(0, 0, 77, 2, 9);
    ASSERT_GE(token, 0);
    ASSERT_EQ(C.votesSent.size(), 1u);
    rune::replication::TxnVoteMsg wire{};
    ASSERT_TRUE(rune::replication::TxnVoteMsg::decode(C.votesSent[0].second.data(), C.votesSent[0].second.size(), wire));
    EXPECT_EQ(wire.token, token);
    EXPECT_EQ(wire.coordinatorNodeId, 0);
    EXPECT_EQ(wire.coordinatorEpoch, 9);
    EXPECT_EQ(wire.leaderEpoch, 1) << "the image's leader epoch rides with the vote";
    EXPECT_EQ(wire.producerId, 77);
    EXPECT_EQ(wire.producerEpoch, 2);
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 1u);
    EXPECT_TRUE(C.got.empty()) << "never answered synchronously";
    const auto firstCopy = C.votesSent.back();
    ferry(C.votesSent, tr1, 1);
    EXPECT_EQ(L.gw.remoteTxnVotesForTest(), 1u);
    tr1.deliverFrameForTest(firstCopy.second.data(), firstCopy.second.size(), 1);   // a re-sent copy while the answer is owed
    L.runWorker(2);
    EXPECT_EQ(L.voteReturns, 1) << "a copy of a pending token must not park a second vote";
    EXPECT_EQ(L.gw.remoteTxnVotesForTest(), 0u);
    ferry(L.responsesSent, tr0, 0);
    ASSERT_EQ(C.got.size(), 1u);
    EXPECT_EQ(C.got[0].producerId, 77);
    EXPECT_EQ(C.got[0].producerEpoch, 2);
    EXPECT_EQ(C.got[0].coordinatorEpoch, 9);
    EXPECT_EQ(C.got[0].r.errorCode, 0);
    EXPECT_EQ(C.got[0].r.code, 0u);
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 0u);

    // A no vote: the worker's fields map onto the response, behind a plane that first refuses it.
    ASSERT_GE(C.gw.sendTxnVote(0, 0, 78, 1, 9), 0);
    ferry(C.votesSent, tr1, 1);
    EXPECT_EQ(L.gw.remoteTxnVotesForTest(), 1u);
    WorkerReturn ret;
    ret.type = WorkerReturn::Type::TxnVoteResult;
    ret.correlationId = C.gw.lastVoteTokenForTest();
    ret.sessionId = 0;
    ret.recordCount = 55;
    ret.newNextOffset = 0x00070000;
    ret.baseOffset = 123;
    L.acceptResponses = false;   // the plane refuses: the answer waits in the FIFO
    L.gw.handleWorkerReturnForTest(ret);
    EXPECT_EQ(L.gw.remoteTxnVotesForTest(), 0u);
    EXPECT_EQ(L.gw.txnVoteResponsesQueuedForTest(), 1u);
    EXPECT_TRUE(L.responsesSent.empty());
    L.acceptResponses = true;
    (void)L.gw.pumpTxnMarkersForTest(3);
    EXPECT_EQ(L.gw.txnVoteResponsesQueuedForTest(), 0u);
    const auto answer = L.responsesSent.back();
    ferry(L.responsesSent, tr0, 0);
    ASSERT_EQ(C.got.size(), 2u);
    EXPECT_EQ(C.got[1].producerId, 78);
    EXPECT_EQ(C.got[1].coordinatorEpoch, 9);
    EXPECT_EQ(C.got[1].r.errorCode, 0);
    EXPECT_EQ(C.got[1].r.code, 55u);
    EXPECT_EQ(C.got[1].r.constraintId, 0x00070000u);
    EXPECT_EQ(C.got[1].r.failingOffset, 123);
    tr0.deliverFrameForTest(answer.second.data(), answer.second.size(), 1);   // a late duplicate answer
    EXPECT_EQ(C.got.size(), 2u);

    // A partition the leader does not serve is answered NotLeader at once, without the worker.
    ASSERT_GE(C.gw.sendTxnVote(0, 1, 79, 1, 9), 0);
    ferry(C.votesSent, tr1, 1);
    EXPECT_EQ(L.gw.remoteTxnVotesForTest(), 0u);
    ferry(L.responsesSent, tr0, 0);
    ASSERT_EQ(C.got.size(), 3u);
    EXPECT_EQ(C.got[2].r.errorCode, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader));
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 0u);

    // The plane refuses the vote: -1, nothing kept, no token drawn.
    C.acceptVotes = false;
    const int64_t last = C.gw.lastVoteTokenForTest();
    EXPECT_EQ(C.gw.sendTxnVote(0, 0, 80, 1, 9), -1);
    EXPECT_EQ(C.gw.lastVoteTokenForTest(), last);
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 0u);
}

// A vote routed under an older leader epoch of the partition is refused NotLeader; the current epoch votes.
TEST(CheckedWritesTxnVoteRouting, OlderLeaderEpochIsRefusedNotLeader) {
    VoteRig L(2);
    WorkerOp op;   // re-elected: leader epoch 1 -> 3
    op.type = WorkerOp::Type::BecomeLeader; op.topicId = 0; op.partition = 0; op.replica = L.replica;
    op.roleEpoch = 3; op.isr = {2};
    ASSERT_TRUE(L.pool.tryEnqueueOp(0, 0, std::move(op)));
    L.runWorker(5);
    ASSERT_EQ(L.replica->role(), Role::Leader);
    rune::replication::TxnVoteMsg m{};
    m.coordinatorNodeId = 0; m.coordinatorEpoch = 9; m.token = 914; m.topicId = 0; m.partition = 0;
    m.producerId = 77; m.producerEpoch = 1; m.leaderEpoch = 1;   // routed while epoch 1 was current
    L.gw.onRemoteTxnVote(m);
    for (int i = 0; i < 16 && L.responsesSent.empty(); ++i) L.runWorker(10 + i);
    ASSERT_EQ(L.responsesSent.size(), 1u);
    rune::replication::TxnVoteResponseMsg r{};
    ASSERT_TRUE(rune::replication::TxnVoteResponseMsg::decode(L.responsesSent[0].second.data(), L.responsesSent[0].second.size(), r));
    EXPECT_EQ(L.responsesSent[0].first, 0);
    EXPECT_EQ(r.token, 914);
    EXPECT_EQ(r.coordinatorEpoch, 9);
    EXPECT_EQ(r.errorCode, static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader));
    m.token = 915; m.leaderEpoch = 3;
    L.gw.onRemoteTxnVote(m);
    for (int i = 0; i < 16 && L.responsesSent.size() < 2; ++i) L.runWorker(30 + i);
    ASSERT_EQ(L.responsesSent.size(), 2u);
    ASSERT_TRUE(rune::replication::TxnVoteResponseMsg::decode(L.responsesSent[1].second.data(), L.responsesSent[1].second.size(), r));
    EXPECT_EQ(r.token, 915);
    EXPECT_EQ(r.errorCode, 0);
    EXPECT_EQ(r.code, 0u);
    EXPECT_EQ(L.gw.remoteTxnVotesForTest(), 0u);
}
