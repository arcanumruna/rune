// Checked writes Part 11, Task 9: routing TxnVote through the gateway.  A real WorkerPool and worker behind a real
// ClientGateway; no cluster plane and no Aeron driver.
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <gtest/gtest.h>

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
    bool resendInSink = false;
    std::vector<int64_t> reentrantTokens;
    std::size_t inFlightAtSink = 99;
    VoteRig() {
        PartitionConfig c; c.topicId = 0; c.partitionId = 0; c.selfId = 0; c.minIsr = 1; c.durabilityTier = 1;
        c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL; c.pushOnAppend = false;
        c.replicaLagMaxMs = 1'000'000;
        reg.add(0, 0, std::make_unique<PartitionReplica>(c, Role::Leader, 1, std::vector<NodeId>{0}, std::vector<NodeId>{0},
                                                         topics.log(0, 0), &t));
        pool.assignFromRegistry(reg);
        pool.setRegistry(&reg);
        pool.setRunnersStartedForTest(true);
        gw.setNodeIdForTest(0);
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
        if (rig->resendInSink) {   // enough emplaces to rehash the table while the gateway is inside the drain
            rig->resendInSink = false;
            for (int i = 0; i < 8; ++i)
                rig->reentrantTokens.push_back(rig->gw.sendTxnVote(topicId, partition, producerId + 100, producerEpoch, coordinatorEpoch));
        }
    }
    void runWorker(int64_t now) {
        pool.worker(0).doWork(now);
        pool.worker(0).drainReturn([&](WorkerReturn ret) { gw.handleWorkerReturnForTest(ret); });
    }
};
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
    while (sent < 100000 && R.gw.sendTxnVote(0, 0, 77 + static_cast<int64_t>(sent), 1, 9) >= 0) ++sent;
    ASSERT_LT(sent, 100000u) << "the ring never filled";
    ASSERT_GT(sent, 0u);
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), sent);
    for (int pass = 0; pass < 1000 && R.gw.txnVotesInFlightForTest() > 0; ++pass) R.runWorker(2 + pass);
    EXPECT_EQ(R.got.size(), sent);
    EXPECT_EQ(R.gw.txnVotesInFlightForTest(), 0u);
}
