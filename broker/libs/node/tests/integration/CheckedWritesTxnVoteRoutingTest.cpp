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
#include <rune/node/TransactionCoordinationService.h>
#include <rune/node/TxnVoteWiring.h>
#include <rune/protocol/client/TxnMessages.h>
#include <rune/storage/Log.h>
#include <rune/transaction/ReplicatedTxnStateStore.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/ReplicationMessages.h>
#include <gtest/gtest.h>
#include <Aeron.h>
#include <Context.h>

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <span>
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

    // The transport's own sends: a peer is accepted, this node is refused, and the thunks keep (self, to, msg) order.
    rune::replication::TxnVoteResponseMsg ans{};
    ASSERT_TRUE(rune::replication::TxnVoteResponseMsg::decode(answer.second.data(), answer.second.size(), ans));
    EXPECT_TRUE(tr0.sendTxnVote(1, wire));
    EXPECT_FALSE(tr0.sendTxnVote(0, wire));
    EXPECT_TRUE(tr0.sendTxnVoteResponse(1, ans));
    EXPECT_FALSE(tr0.sendTxnVoteResponse(0, ans));
    EXPECT_TRUE(rune::replication::AeronPartitionTransport::sendTxnVoteThunk(&tr0, 1, wire));
    EXPECT_FALSE(rune::replication::AeronPartitionTransport::sendTxnVoteThunk(&tr0, 0, wire));
    EXPECT_TRUE(rune::replication::AeronPartitionTransport::sendTxnVoteResponseThunk(&tr0, 1, ans));
    EXPECT_FALSE(rune::replication::AeronPartitionTransport::sendTxnVoteResponseThunk(&tr0, 0, ans));

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

// An answer completes a vote only when every field matches and the vote went to a remote leader.
TEST(CheckedWritesTxnVoteRoutingRegression, AnswerDifferingInAnyFieldIsIgnoredRegression) {
    VoteRig C(0);
    C.gw.setRoutingImageSnapshot(voteImage(1, 1));
    const int64_t token = C.gw.sendTxnVote(0, 0, 77, 2, 9);
    ASSERT_GE(token, 0);
    rune::replication::TxnVoteResponseMsg r{};
    r.token = token; r.topicId = 0; r.partition = 0; r.producerId = 77; r.producerEpoch = 2; r.coordinatorEpoch = 9;
    auto stale = r; stale.coordinatorEpoch = 8;
    C.gw.onRemoteTxnVoteResponse(stale);
    stale = r; stale.producerEpoch = 3;
    C.gw.onRemoteTxnVoteResponse(stale);
    stale = r; stale.topicId = 1;
    C.gw.onRemoteTxnVoteResponse(stale);
    stale = r; stale.partition = 1;
    C.gw.onRemoteTxnVoteResponse(stale);
    stale = r; stale.producerId = 78;
    C.gw.onRemoteTxnVoteResponse(stale);
    EXPECT_TRUE(C.got.empty()) << "a mismatched answer completed the vote";
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 1u);
    C.gw.onRemoteTxnVoteResponse(r);
    EXPECT_EQ(C.got.size(), 1u);
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 0u);

    VoteRig N(0);   // no image: the vote goes to the local worker, and a plane answer naming its token is not its answer
    const int64_t local = N.gw.sendTxnVote(0, 0, 77, 2, 9);
    ASSERT_GE(local, 0);
    r.token = local;
    N.gw.onRemoteTxnVoteResponse(r);
    EXPECT_TRUE(N.got.empty());
    EXPECT_EQ(N.gw.txnVotesInFlightForTest(), 1u);
    N.runWorker(2);
    EXPECT_EQ(N.got.size(), 1u);
}

// A vote naming no possible origin (negative, or this node) is dropped: no entry, no queued answer, no op.
TEST(CheckedWritesTxnVoteRoutingRegression, VoteFromAnImpossibleOriginIsDroppedRegression) {
    VoteRig L(1);
    rune::replication::TxnVoteMsg m{};
    m.coordinatorEpoch = 9; m.token = 914; m.topicId = 0; m.partition = 0; m.producerId = 77; m.producerEpoch = 1;
    for (const int32_t origin : {-1, 1}) {
        m.coordinatorNodeId = origin;
        L.gw.onRemoteTxnVote(m);
        L.runWorker(2);
        EXPECT_EQ(L.gw.remoteTxnVotesForTest(), 0u) << "origin " << origin;
        EXPECT_EQ(L.gw.txnVoteResponsesQueuedForTest(), 0u) << "origin " << origin;
        EXPECT_TRUE(L.responsesSent.empty()) << "origin " << origin;
        EXPECT_EQ(L.voteReturns, 0) << "origin " << origin;
    }
}

namespace {
// Partition 0 led by `leader0` at `epoch0`; partition 1 led by this node (node 0), so its votes go to the local worker.
std::shared_ptr<const rune::metadata::RoutingImage> voteImageSplit(int32_t leader0, int32_t epoch0) {
    return rune::metadata::RoutingImage::build({{rune::metadata::gpidOf(0, 0), leader0, epoch0},
                                                {rune::metadata::gpidOf(0, 1), 0, 1}});
}
constexpr int16_t kVoteNotLeader = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
}

// A remote vote completes NotLeader when its leader or leader epoch changes in the image, and only then.
TEST(CheckedWritesTxnVoteRoutingRegression, LeaderChangeFailsInFlightVotesNotLeaderRegression) {
    VoteRig C(0);
    C.gw.setRoutingImageSnapshot(voteImageSplit(1, 1));
    const int64_t remote = C.gw.sendTxnVote(0, 0, 77, 2, 9);
    ASSERT_GE(remote, 0);
    ASSERT_EQ(C.votesSent.size(), 1u);
    ASSERT_GE(C.gw.sendTxnVote(0, 1, 78, 1, 9), 0);   // local
    ASSERT_EQ(C.gw.txnVotesInFlightForTest(), 2u);

    C.gw.setRoutingImageSnapshot(voteImageSplit(1, 1));   // identical re-publish
    (void)C.gw.pumpTxnMarkersForTest(2);
    EXPECT_TRUE(C.got.empty()) << "an unchanged image failed a vote";
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 2u);

    C.gw.setRoutingImageSnapshot(voteImageSplit(1, 2));   // epoch bump
    (void)C.gw.pumpTxnMarkersForTest(3);
    ASSERT_EQ(C.got.size(), 1u);
    EXPECT_EQ(C.got[0].partition, 0);
    EXPECT_EQ(C.got[0].producerId, 77);
    EXPECT_EQ(C.got[0].producerEpoch, 2);
    EXPECT_EQ(C.got[0].coordinatorEpoch, 9);
    EXPECT_EQ(C.got[0].r.errorCode, kVoteNotLeader);
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 1u) << "the local vote is the worker's to release";

    rune::replication::TxnVoteResponseMsg late{};   // the old leader's answer arrives after the failure
    late.token = remote; late.topicId = 0; late.partition = 0; late.producerId = 77; late.producerEpoch = 2; late.coordinatorEpoch = 9;
    C.gw.onRemoteTxnVoteResponse(late);
    EXPECT_EQ(C.got.size(), 1u);

    C.runWorker(4);   // the local vote is released by the worker (partition 1 has no replica)
    ASSERT_EQ(C.got.size(), 2u);
    EXPECT_EQ(C.got[1].partition, 1);
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 0u);

    // Leader moved to another node, and then to this one; the sink sends again from inside the sweep.
    const int64_t moved = C.gw.sendTxnVote(0, 0, 79, 1, 9);
    ASSERT_GE(moved, 0);
    C.resendInSink = true;
    C.gw.setRoutingImageSnapshot(voteImageSplit(2, 5));
    (void)C.gw.pumpTxnMarkersForTest(5);
    ASSERT_EQ(C.got.size(), 3u);
    EXPECT_EQ(C.got[2].r.errorCode, kVoteNotLeader);
    EXPECT_EQ(C.inFlightAtSink, 0u) << "the failed entry is erased before the sink runs";
    ASSERT_EQ(C.reentrantTokens.size(), 8u);
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 8u);
    C.gw.setRoutingImageSnapshot(voteImageSplit(0, 6));   // now led here: every remote entry fails
    (void)C.gw.pumpTxnMarkersForTest(6);
    EXPECT_EQ(C.got.size(), 11u);
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 0u);
}

// A plane reconnect re-sends the in-flight votes of that peer with the same token; the answer completes once.
TEST(CheckedWritesTxnVoteRoutingRegression, ReconnectResendsWithTheSameTokenRegression) {
    VoteRig C(0);
    C.gw.setRoutingImageSnapshot(voteImageSplit(1, 1));
    const int64_t token = C.gw.sendTxnVote(0, 0, 77, 2, 9);
    ASSERT_GE(token, 0);
    ASSERT_EQ(C.votesSent.size(), 1u);
    const auto first = C.votesSent[0];
    C.votesSent.clear();

    C.gw.onPlaneReconnect(3);   // a peer this vote did not go to
    (void)C.gw.pumpTxnMarkersForTest(2);
    EXPECT_TRUE(C.votesSent.empty());

    C.gw.onPlaneReconnect(1);
    (void)C.gw.pumpTxnMarkersForTest(3);
    ASSERT_EQ(C.votesSent.size(), 1u);
    EXPECT_EQ(C.votesSent[0].first, 1);
    EXPECT_EQ(C.votesSent[0].second, first.second) << "the re-send is the same frame: same token, fields and epoch";
    C.votesSent.clear();

    C.gw.onPlaneReconnect(-1);
    (void)C.gw.pumpTxnMarkersForTest(4);
    ASSERT_EQ(C.votesSent.size(), 1u);
    EXPECT_EQ(C.votesSent[0].first, 1);
    C.votesSent.clear();
    (void)C.gw.pumpTxnMarkersForTest(5);   // the events were consumed
    EXPECT_TRUE(C.votesSent.empty());
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 1u);

    rune::replication::TxnVoteResponseMsg r{};   // answers to two copies
    r.token = token; r.topicId = 0; r.partition = 0; r.producerId = 77; r.producerEpoch = 2; r.coordinatorEpoch = 9;
    C.gw.onRemoteTxnVoteResponse(r);
    C.gw.onRemoteTxnVoteResponse(r);
    EXPECT_EQ(C.got.size(), 1u);
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 0u);

    // The plane refuses the re-send: the vote completes NotLeader.
    ASSERT_GE(C.gw.sendTxnVote(0, 0, 78, 1, 9), 0);
    C.votesSent.clear();
    C.acceptVotes = false;
    C.gw.onPlaneReconnect(1);
    (void)C.gw.pumpTxnMarkersForTest(6);
    ASSERT_EQ(C.got.size(), 2u);
    EXPECT_EQ(C.got[1].producerId, 78);
    EXPECT_EQ(C.got[1].r.errorCode, kVoteNotLeader);
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 0u);

    // A local vote is not re-sent and not failed by a reconnect.
    ASSERT_GE(C.gw.sendTxnVote(0, 1, 80, 1, 9), 0);
    C.acceptVotes = true;
    C.votesSent.clear();
    C.gw.onPlaneReconnect(-1);
    (void)C.gw.pumpTxnMarkersForTest(7);
    EXPECT_TRUE(C.votesSent.empty()) << "a local vote is never sent over the plane";
    EXPECT_EQ(C.got.size(), 2u);
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 1u);

    // Leader side: a copy of a pending token is not enqueued twice.
    VoteRig L(1);
    rune::replication::TxnVoteMsg m{};
    m.coordinatorNodeId = 0; m.coordinatorEpoch = 9; m.token = 914; m.topicId = 0; m.partition = 0;
    m.producerId = 77; m.producerEpoch = 1; m.leaderEpoch = 1;
    L.gw.onRemoteTxnVote(m);
    L.gw.onRemoteTxnVote(m);
    L.runWorker(2);
    EXPECT_EQ(L.voteReturns, 1);
    EXPECT_EQ(L.responsesSent.size(), 1u);
}

// purgeTxnVotes silently drops the votes of coordinator epochs below the bound.
TEST(CheckedWritesTxnVoteRoutingRegression, PurgeDropsVotesOfOlderCoordinatorEpochsRegression) {
    VoteRig C(0);
    C.gw.setRoutingImageSnapshot(voteImageSplit(1, 1));
    const int64_t old = C.gw.sendTxnVote(0, 0, 77, 2, 5);
    ASSERT_GE(C.gw.sendTxnVote(0, 0, 78, 2, 9), 0);
    ASSERT_GE(C.gw.sendTxnVote(0, 1, 79, 2, 4), 0);   // local, old epoch
    EXPECT_EQ(C.gw.purgeTxnVotes(7), 2u);
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 1u);
    EXPECT_EQ(C.gw.purgeTxnVotes(7), 0u);
    rune::replication::TxnVoteResponseMsg r{};
    r.token = old; r.topicId = 0; r.partition = 0; r.producerId = 77; r.producerEpoch = 2; r.coordinatorEpoch = 5;
    C.gw.onRemoteTxnVoteResponse(r);
    C.runWorker(2);   // the purged local vote's answer is dropped too
    EXPECT_TRUE(C.got.empty()) << "a purged vote produces no result";
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 1u);
}

// BrokerNode's wiring (TxnVoteWiring.h): the candidate set, and the vote sink / predicate on the real gateway.
TEST(CheckedWritesTxnVoteRouting, CandidateSetHoldsKeyedTopicsAndEveryViewSource) {
    VoteCandidateTopics c;
    c.noteWriteSpec(1, true);
    c.noteWriteSpec(2, false);   // a typed topic without key.index
    const std::vector<int32_t> sources = {4, 5, 6};
    c.noteViewSources(sources);
    EXPECT_TRUE(c.contains(1));
    EXPECT_FALSE(c.contains(2));
    EXPECT_FALSE(c.contains(3));   // a plain topic
    for (const int32_t s : sources) EXPECT_TRUE(c.contains(s));
    c.forget(1);
    c.forget(5);
    EXPECT_FALSE(c.contains(1));
    EXPECT_FALSE(c.contains(5));
    EXPECT_TRUE(c.contains(4));
    EXPECT_TRUE(c.contains(6));
}

namespace {
namespace pc = rune::protocol::client;
// A store-backed coordinator wired to the rig's gateway the way BrokerNode does, with one open transaction on topic 0.
struct CheckedWritesTxnVoteRoutingWired : ::testing::Test {
    VoteRig R;
    VoteTmpDir logDir;
    rune::storage::Log txnLog{logDir.path.string()};
    PartitionConfig c = [] {
        PartitionConfig c; c.topicId = 0; c.partitionId = 0; c.selfId = 1; c.minIsr = 1; c.durabilityTier = 1;
        c.fetchIntervalNs = 1'000'000LL; c.isrCheckIntervalNs = 1'000'000LL; c.pushOnAppend = false; c.replicaLagMaxMs = 1'000'000;
        return c;
    }();
    PartitionReplica txnReplica{c, Role::Leader, 1, std::vector<NodeId>{1}, std::vector<NodeId>{1}, &txnLog, &R.t};
    rune::transaction::ReplicatedTxnStateStore store{&txnReplica};
    bool leader = true;
    std::unique_ptr<TransactionCoordinationService> tcs = std::make_unique<TransactionCoordinationService>(
        &store, &R.topics, [this] { return leader; }, [] { return NodeId{1}; });
    VoteCandidateTopics candidates;
    int64_t now = 1'000'000'000;
    std::array<uint8_t, 1024> req{}, out{};
    rune::coordination::NsRequestContext ctx{};
    pc::EndTxn end;
    std::vector<std::vector<uint8_t>> frames;
    const std::string txn = "tx";
    int64_t pid = 0;
    int16_t ep = 0;

    // The candidates are judged when the partition is added, so a keyed topic is noted before.
    void begin(bool keyed) {
        if (keyed) candidates.noteWriteSpec(0, true);
        R.gw.setOnControlFrame([this](int64_t, const uint8_t* f, std::size_t n) { frames.emplace_back(f, f + n); });
        tcs->setGateway(&R.gw);
        wireTxnVote(R.gw, *tcs, candidates);
        for (int i = 0; i < 20 && !tcs->isLoaded(); ++i) step();
        ASSERT_TRUE(tcs->isLoaded());
        const std::span<const uint8_t> txnBytes(reinterpret_cast<const uint8_t*>(txn.data()), txn.size());
        pc::InitProducerId init;
        init.sessionId = 1; init.correlationId = 1; init.transactionTimeoutMs = 600'000; init.transactionalId = txnBytes;
        const std::size_t initN = tcs->handleInitProducerId(req.data(), init.encode(req.data()), now, out.data(), out.size(), ctx);
        step(); step();
        ASSERT_TRUE(initN != 0 || !frames.empty());
        const auto ip = initN != 0 ? pc::InitProducerIdResponse::decode(out.data(), initN)
                                   : pc::InitProducerIdResponse::decode(frames.back().data(), frames.back().size());
        ASSERT_TRUE(ip && ip->errorCode == 0);
        pid = ip->producerId;
        ep = ip->producerEpoch;
        pc::AddPartitionsToTxn add;
        add.sessionId = 1; add.correlationId = 2; add.producerId = pid; add.producerEpoch = ep; add.transactionalId = txnBytes;
        const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
        const std::size_t addN = tcs->handleAddPartitions(req.data(), add.encode(req.data(), parts), now, out.data(), out.size(), ctx);
        step(); step();
        ASSERT_TRUE(addN != 0 || !frames.empty());
        const auto ar = addN != 0 ? pc::AddPartitionsToTxnResponse::decode(out.data(), addN)
                                  : pc::AddPartitionsToTxnResponse::decode(frames.back().data(), frames.back().size());
        ASSERT_TRUE(ar && ar->results.size() == 1 && ar->results[0].errorCode == 0);
        end.sessionId = 1; end.correlationId = 3; end.producerId = pid; end.producerEpoch = ep; end.commit = 1; end.transactionalId = txnBytes;
    }
    void step() { now += 1'000'000; txnReplica.doWork(now); if (tcs) (void)tcs->doWork(now); }
    std::size_t endTxn() { return tcs->handleEndTxn(req.data(), end.encode(req.data()), now, out.data(), out.size(), ctx); }
};
using CheckedWritesTxnVoteRoutingWiredRegression = CheckedWritesTxnVoteRoutingWired;
} // namespace

// The coordinator's load and unload purge the gateway's votes.  An unload and reload at the SAME coordinator epoch (a
// lost coordinator check) must not let the old load's vote count for the retried commit, and a vote of an older epoch
// is dropped by the load.
TEST_F(CheckedWritesTxnVoteRoutingWiredRegression, LoadAndUnloadPurgeTheGatewaysVotesRegression) {
    ASSERT_NO_FATAL_FAILURE(begin(true));
    ASSERT_EQ(endTxn(), 0u);
    ASSERT_EQ(R.gw.txnVotesInFlightForTest(), 1u);
    const int64_t oldToken = R.gw.lastVoteTokenForTest();
    const int32_t epochBefore = tcs->coordinatorEpoch();

    leader = false;   // the coordinator check is lost: unload
    step();
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 0u) << "the unload purges the gateway's votes of this load";
    // A vote of an older coordinator epoch is still out while unloaded: the next load drops it.
    ASSERT_GE(R.gw.sendTxnVote(0, 0, pid + 1, ep, epochBefore - 1), 0);
    ASSERT_EQ(R.gw.txnVotesInFlightForTest(), 1u);
    leader = true;
    step(); step();
    ASSERT_TRUE(tcs->isLoaded());
    ASSERT_EQ(tcs->coordinatorEpoch(), epochBefore) << "a reload at the same epoch";
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 0u) << "the load purges the older epochs' votes";
    ASSERT_EQ(tcs->txnStateForTest("tx"), rune::transaction::TxnState::Ongoing);

    end.correlationId = 4;   // the client's retry asks every partition again
    ASSERT_EQ(endTxn(), 0u);
    ASSERT_EQ(R.gw.txnVotesInFlightForTest(), 1u);
    WorkerReturn late;   // the old load's vote answers a no
    late.type = WorkerReturn::Type::TxnVoteResult;
    late.correlationId = oldToken;
    late.sessionId = -1;
    late.recordCount = 55;
    late.newNextOffset = 0x00070000;
    late.baseOffset = 123;
    R.gw.handleWorkerReturnForTest(late);
    step();
    EXPECT_EQ(tcs->txnStateForTest("tx"), rune::transaction::TxnState::Ongoing) << "the old load's answer does not count for the retried gate";
    EXPECT_EQ(tcs->voteGatesForTest(), 1u);
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 1u);
}

TEST_F(CheckedWritesTxnVoteRoutingWired, CommitOnACandidateSendsAVoteThroughTheGatewayAndTheSinkAnswersIt) {
    ASSERT_NO_FATAL_FAILURE(begin(true));
    ASSERT_EQ(endTxn(), 0u);
    ASSERT_EQ(R.gw.txnVotesInFlightForTest(), 1u);
    R.runWorker(2);   // the leader answers yes into the coordinator, through the gateway's sink
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 0u);
    for (int i = 0; i < 4; ++i) step();
    EXPECT_EQ(tcs->txnStateForTest("tx"), rune::transaction::TxnState::PrepareCommit) << "the yes reached the gate";
}

TEST_F(CheckedWritesTxnVoteRoutingWired, CommitOnAPlainTopicAsksNoVote) {
    ASSERT_NO_FATAL_FAILURE(begin(false));
    const std::size_t n = endTxn();
    ASSERT_NE(n, 0u) << "answered at once";
    const auto r = pc::EndTxnResponse::decode(out.data(), n);
    ASSERT_TRUE(r);
    EXPECT_EQ(r->errorCode, 0);
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 0u);
    EXPECT_EQ(tcs->voteGatesForTest(), 0u);
    EXPECT_EQ(tcs->txnStateForTest("tx"), rune::transaction::TxnState::PrepareCommit);
}

// The setup-failure path and the teardown both release through releaseTxnCoordinator: the gateway must not keep the dead coordinator.
TEST_F(CheckedWritesTxnVoteRoutingWiredRegression, ReleaseDetachesTheGatewayFromTheCoordinatorRegression) {
    ASSERT_NO_FATAL_FAILURE(begin(true));
    ASSERT_EQ(endTxn(), 0u);
    ASSERT_EQ(R.gw.txnVotesInFlightForTest(), 1u);
    ASSERT_TRUE(R.gw.txnVoteSinkSetForTest());
    ASSERT_NE(R.gw.txnService(), nullptr);
    releaseTxnCoordinator(&R.gw, tcs);
    EXPECT_EQ(tcs, nullptr);
    EXPECT_EQ(R.gw.txnService(), nullptr) << "no dangling handler";
    EXPECT_FALSE(R.gw.txnVoteSinkSetForTest()) << "no dangling sink";
    R.runWorker(2);   // the late answer of an in-flight vote reaches no one
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 0u);
}

// #995: an interim return (the judge waits on another transaction) reaches the sink as `waiting` and leaves the vote in flight; the
// real answer that follows is the one sink call that completes it.
TEST(CheckedWritesTxnVoteRoutingRegression, AnInterimIsPassedOnAndTheVoteStaysInFlightRegression) {
    VoteRig R;
    ASSERT_GE(R.gw.sendTxnVote(0, 0, 78, 1, 9), 0);
    WorkerReturn ret;
    ret.type = WorkerReturn::Type::TxnVoteResult;
    ret.correlationId = R.gw.lastVoteTokenForTest();
    ret.sessionId = -1;
    ret.errorCode = rune::replication::kTxnVoteWaiting;
    ret.baseOffset = 900;      // the holder's producer id
    ret.newNextOffset = 2;     // its epoch
    R.gw.handleWorkerReturnForTest(ret);
    ASSERT_EQ(R.got.size(), 1u);
    EXPECT_TRUE(R.got[0].r.waiting);
    EXPECT_EQ(R.got[0].r.waitPid, 900);
    EXPECT_EQ(R.got[0].r.waitEpoch, 2);
    EXPECT_EQ(R.got[0].producerId, 78);
    EXPECT_EQ(R.got[0].coordinatorEpoch, 9);
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 1u) << "an interim is not the answer";
    R.gw.handleWorkerReturnForTest(ret);   // a second one (the holder changed) is passed on too
    EXPECT_EQ(R.got.size(), 2u);
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 1u);

    ret.errorCode = 0;                     // the real vote
    ret.recordCount = 0;
    ret.newNextOffset = 0;
    ret.baseOffset = -1;
    R.gw.handleWorkerReturnForTest(ret);
    ASSERT_EQ(R.got.size(), 3u);
    EXPECT_FALSE(R.got[2].r.waiting);
    EXPECT_EQ(R.got[2].r.errorCode, 0);
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 0u);
    ret.errorCode = rune::replication::kTxnVoteWaiting;   // a late interim after the answer matches nothing
    R.gw.handleWorkerReturnForTest(ret);
    EXPECT_EQ(R.got.size(), 3u);
}

// The same across the plane: the leader's interim goes to the coordinator's node as a TxnVoteResponse with errorCode kTxnVoteWaiting,
// keeps the remote vote owed on the leader and the vote in flight on the coordinator.
TEST(CheckedWritesTxnVoteRoutingRegression, AnInterimCrossesThePlaneAndKeepsTheVoteOwedRegression) {
    VoteTmpDir ad;
    rune::node::EmbeddedDriverConfig dc; dc.dir = ad.path.string(); dc.termBufferBytes = 64 * 1024;
    rune::node::EmbeddedDriver drv(dc, rune::node::DriverMode::Threaded);
    aeron::Context ctx; ctx.aeronDir(ad.path.string());
    auto client = aeron::Aeron::connect(ctx);
    rune::replication::AeronPartitionTransport tr0(client, "aeron:ipc", 7411, 0, 1 << 20);
    rune::replication::AeronPartitionTransport tr1(client, "aeron:ipc", 7412, 1, 1 << 20);
    VoteRig C(0), L(1);
    tr0.setTxnVoteHandlers(&rune::gateway::ClientGateway::onTxnVoteThunk,
                           &rune::gateway::ClientGateway::onTxnVoteResponseThunk, &C.gw);
    tr1.setTxnVoteHandlers(&rune::gateway::ClientGateway::onTxnVoteThunk,
                           &rune::gateway::ClientGateway::onTxnVoteResponseThunk, &L.gw);
    C.gw.setRoutingImageSnapshot(voteImage(1, 1));
    auto ferry = [](auto& out, rune::replication::AeronPartitionTransport& to) {
        ASSERT_FALSE(out.empty());
        to.deliverFrameForTest(out.back().second.data(), out.back().second.size(), 1);
        out.clear();
    };
    ASSERT_GE(C.gw.sendTxnVote(0, 0, 78, 1, 9), 0);
    ferry(C.votesSent, tr1);
    ASSERT_EQ(L.gw.remoteTxnVotesForTest(), 1u);
    WorkerReturn ret;
    ret.type = WorkerReturn::Type::TxnVoteResult;
    ret.correlationId = C.gw.lastVoteTokenForTest();
    ret.sessionId = 0;
    ret.errorCode = rune::replication::kTxnVoteWaiting;
    ret.baseOffset = 900;
    ret.newNextOffset = 2;
    L.gw.handleWorkerReturnForTest(ret);
    EXPECT_EQ(L.gw.remoteTxnVotesForTest(), 1u) << "the answer is still owed";
    ASSERT_EQ(L.responsesSent.size(), 1u);
    rune::replication::TxnVoteResponseMsg wire{};
    ASSERT_TRUE(rune::replication::TxnVoteResponseMsg::decode(L.responsesSent[0].second.data(), L.responsesSent[0].second.size(), wire));
    EXPECT_EQ(wire.errorCode, rune::replication::kTxnVoteWaiting);
    EXPECT_EQ(wire.failingOffset, 900);
    EXPECT_EQ(wire.constraintId, 2u);
    ferry(L.responsesSent, tr0);
    ASSERT_EQ(C.got.size(), 1u);
    EXPECT_TRUE(C.got[0].r.waiting);
    EXPECT_EQ(C.got[0].r.waitPid, 900);
    EXPECT_EQ(C.got[0].r.waitEpoch, 2);
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 1u);

    ret.errorCode = 0;   // the real vote
    ret.baseOffset = -1;
    ret.newNextOffset = 0;
    L.gw.handleWorkerReturnForTest(ret);
    EXPECT_EQ(L.gw.remoteTxnVotesForTest(), 0u);
    ferry(L.responsesSent, tr0);
    ASSERT_EQ(C.got.size(), 2u);
    EXPECT_FALSE(C.got[1].r.waiting);
    EXPECT_EQ(C.gw.txnVotesInFlightForTest(), 0u);
}
