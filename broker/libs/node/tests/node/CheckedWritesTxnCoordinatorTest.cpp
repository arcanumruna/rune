// ---------------------------------------------------------------------------
// CheckedWritesTxnCoordinatorTest.cpp — checked writes Part 11, Task 10a: the transaction coordinator's vote gate
// before PrepareCommit.  EndTxn(commit) of a transaction with candidate partitions persists nothing until every
// candidate voted yes (the usual PrepareCommit path) or one voted no (PrepareAbort, the no is the answer); either
// way the EndTxn is answered once the markers landed.
//
// The in-process rig of TxnStateReplicationTest (ISR {1,2}: a record commits when node 2 fetched it) plus a
// TopicRegistry so topic ids and names resolve.  Votes and markers are captured by the test seams; no gateway, no
// Aeron.  The helpers live in an anonymous namespace: this file links into rune_txn_coordination_service_tests next
// to TransactionCoordinationServiceTest.cpp, whose TmpDir / NullTransport are global.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/TransactionCoordinationService.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/TxnMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/storage/Log.h>
#include <rune/transaction/ReplicatedTxnStateStore.h>

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

namespace pc  = rune::protocol::client;
namespace tx  = rune::transaction;
namespace rep = rune::replication;

namespace {

struct CwTmpDir {
    std::string path;
    CwTmpDir() {
        static std::atomic<uint64_t> seq{0};
        path = std::filesystem::temp_directory_path().string() + "/rune_cwtxn_" + std::to_string(::getpid()) + "_" +
               std::to_string(seq.fetch_add(1)) + "_" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        std::filesystem::create_directories(path);
    }
    ~CwTmpDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

class CwNullTransport : public rep::PartitionTransport {
public:
    bool sendFetchRequest(const rep::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rep::NodeId, const rep::FetchResponseMsg&) override { return true; }
};

rune::node::TopicSpec topicSpec(std::string name, int32_t partitions, uint32_t nsId = 0) {
    rune::node::TopicSpec ts;
    ts.name           = std::move(name);
    ts.partitionCount = partitions;
    ts.nsId           = nsId;
    return ts;
}

rune::coordination::TxnOffsetApply appliedSink(void*, const std::string&, const rune::coordination::TopicPartition&,
                                               int64_t, int64_t, const rune::coordination::TxnOffsetToken&) {
    return rune::coordination::TxnOffsetApply::Applied;
}

struct Answers {
    std::vector<std::vector<uint8_t>> frames;
    static void sink(void* ctx, int64_t, const uint8_t* f, std::size_t n) {
        static_cast<Answers*>(ctx)->frames.emplace_back(f, f + n);
    }
};

struct SentVote {
    std::string topic;
    int32_t     partition;
    int64_t     producerId;
    int16_t     producerEpoch;
};

constexpr int32_t kCw = 0, kPlain = 1, kCwNs = 2, kPlainNs = 3;   // topic ids: the registry numbers topics in order
constexpr uint32_t kNs = 7;   // the namespace of the last two topics

}  // namespace

class CheckedWritesTxnCoordinatorTest : public ::testing::Test {
protected:
    CheckedWritesTxnCoordinatorTest()
        : topicDir_(), logDir_(), log_(logDir_.path),
          topics_({topicSpec("cw", 2), topicSpec("plain", 2), topicSpec("cwn", 2, kNs), topicSpec("plainn", 2, kNs)}, topicDir_.path) {}

    void SetUp() override {
        rep::PartitionConfig cfg;
        cfg.partitionId = 0; cfg.selfId = 1; cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.minIsr = 1; cfg.fetchIntervalNs = 1'000'000; cfg.isrCheckIntervalNs = 1'000'000; cfg.maxFetchBytes = 1 << 20;
        cfg.pushOnAppend = false;
        replica_ = std::make_unique<rep::PartitionReplica>(cfg, rep::Role::Leader, /*epoch=*/1,
                                                           std::vector<rep::NodeId>{1, 2}, std::vector<rep::NodeId>{1, 2},
                                                           &log_, &transport_);
        store_ = std::make_unique<tx::ReplicatedTxnStateStore>(replica_.get());
        svc_ = std::make_unique<rune::node::TransactionCoordinationService>(
            store_.get(), &topics_, [this] { return isLeader_; }, [] { return rune::node::NodeId{1}; });
        svc_->setTxnOffsetSink(&appliedSink, nullptr);
        svc_->setAnswerSinkForTest(&Answers::sink, &answers_);
        svc_->setVoteCandidateFn([this](int32_t topicId) { return (topicId == kCw || topicId == kCwNs) && cwIsCandidate_; });
        svc_->setVoteFnOverride([this](std::string_view topic, int32_t partition, int64_t pid, int16_t epoch) {
            votes_.push_back({std::string(topic), partition, pid, epoch});
            return voteAccepted_;
        });
        svc_->setAppendFnOverride([this](std::string_view, int32_t, std::span<const uint8_t>) -> int64_t {
            ++markerAppends_;
            return appendResult_;
        });
        svc_->setSessionVersionFn(&versionOf, this);
        (void)svc_->doWork(now_);   // loads; requests the first pid block ceiling
        followerFetches();
        (void)svc_->doWork(now_);   // the ceiling is committed
    }

    static int32_t versionOf(void* self, int64_t) noexcept { return static_cast<CheckedWritesTxnCoordinatorTest*>(self)->clientVersion_; }

    /// Node 2 fetches up to the leader's log end: everything appended so far commits.
    void followerFetches() {
        rep::FetchRequestMsg req{};
        req.leaderEpoch = replica_->epoch(); req.partition = 0; req.followerId = 2;
        req.fetchOffset = replica_->logEndOffset(); req.maxBytes = 1 << 20; req.topicId = 0;
        now_ += 1'000'000;
        (void)replica_->onFetchRequest(req, now_);
        replica_->doWork(now_);
    }

    int64_t initPid(const std::string& txn, int16_t& epoch) {
        pc::InitProducerId m;
        m.sessionId = 1; m.correlationId = ++corr_; m.transactionTimeoutMs = 30'000;
        m.transactionalId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(txn.data()), txn.size());
        const std::size_t n = m.encode(reqBuf_.data());
        EXPECT_EQ(svc_->handleInitProducerId(reqBuf_.data(), n, now_, outBuf_.data(), outBuf_.size(), ctx_), 0u);
        followerFetches();
        (void)svc_->doWork(now_);
        EXPECT_EQ(answers_.frames.size(), 1u);
        const auto r = pc::InitProducerIdResponse::decode(answers_.frames.back().data(), answers_.frames.back().size());
        EXPECT_TRUE(r && r->errorCode == 0);
        answers_.frames.clear();
        epoch = r ? r->producerEpoch : 0;
        return r ? r->producerId : -1;
    }

    /// AddPartitions answered once its record committed; returns the first result's code.
    int16_t addPartitions(const std::string& txn, int64_t pid, int16_t epoch, const std::vector<pc::TxnTopicPartition>& parts) {
        pc::AddPartitionsToTxn m;
        m.sessionId = 1; m.correlationId = ++corr_; m.producerId = pid; m.producerEpoch = epoch;
        m.transactionalId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(txn.data()), txn.size());
        const std::size_t n = m.encode(reqBuf_.data(), parts);
        const std::size_t sync = svc_->handleAddPartitions(reqBuf_.data(), n, now_, outBuf_.data(), outBuf_.size(), ctx_);
        if (sync != 0) {   // refused at once (nothing persisted)
            const auto r = pc::AddPartitionsToTxnResponse::decode(outBuf_.data(), sync);
            return r && r->results.size() > 0 ? r->results[0].errorCode : int16_t{-1};
        }
        followerFetches();
        (void)svc_->doWork(now_);
        EXPECT_EQ(answers_.frames.size(), 1u);
        const auto r = pc::AddPartitionsToTxnResponse::decode(answers_.frames.back().data(), answers_.frames.back().size());
        answers_.frames.clear();
        return r && r->results.size() > 0 ? r->results[0].errorCode : int16_t{-1};
    }

    std::size_t endTxn(const std::string& txn, int64_t pid, int16_t epoch, bool commit) {
        pc::EndTxn m;
        m.sessionId = 1; m.correlationId = ++corr_; m.producerId = pid; m.producerEpoch = epoch; m.commit = commit ? 1u : 0u;
        m.transactionalId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(txn.data()), txn.size());
        const std::size_t n = m.encode(reqBuf_.data());
        return svc_->handleEndTxn(reqBuf_.data(), n, now_, outBuf_.data(), outBuf_.size(), ctx_);
    }

    /// The EndTxn answer sent synchronously (outBuf_) or deferred (the last answer frame).
    pc::EndTxnResponse syncAnswer(std::size_t n) {
        const auto r = pc::EndTxnResponse::decode(outBuf_.data(), n);
        EXPECT_TRUE(r.has_value());
        return r.value_or(pc::EndTxnResponse{});
    }
    pc::EndTxnResponse lastAnswer() {
        EXPECT_FALSE(answers_.frames.empty());
        if (answers_.frames.empty()) return {};
        const auto r = pc::EndTxnResponse::decode(answers_.frames.back().data(), answers_.frames.back().size());
        EXPECT_TRUE(r.has_value());
        return r.value_or(pc::EndTxnResponse{});
    }

    void vote(const SentVote& v, uint16_t code = 0, uint32_t constraintId = 0, int64_t offset = -1, int16_t err = 0) {
        rune::gateway::TxnVoteResult r;
        r.errorCode = err; r.code = code; r.constraintId = constraintId; r.failingOffset = offset;
        svc_->onTxnVoteResult(v.topic == "cw" ? kCw : kPlain, v.partition, v.producerId, v.producerEpoch,
                              svc_->coordinatorEpoch(), r);
    }

    void landMarkers(int64_t pid, const std::vector<pc::TxnTopicPartition>& parts) {
        for (const auto& p : parts) svc_->onControlMarkerLanded(p.topicId, p.partition, pid, svc_->coordinatorEpoch());
    }

    /// The decision record commits, the markers go out and land: the transaction is complete.
    void completeWithMarkers(int64_t pid, const std::vector<pc::TxnTopicPartition>& parts) {
        followerFetches();
        (void)svc_->doWork(now_);
        landMarkers(pid, parts);
    }

    /// A transaction on `parts` (txn id "tx"), ready for EndTxn.
    int64_t beginTxn(const std::vector<pc::TxnTopicPartition>& parts, int16_t& epoch, const std::string& txn = "tx") {
        const int64_t pid = initPid(txn, epoch);
        EXPECT_EQ(addPartitions(txn, pid, epoch, parts), 0);
        votes_.clear();
        return pid;
    }

    CwTmpDir topicDir_, logDir_;
    rune::storage::Log log_;
    rune::gateway::TopicRegistry topics_;
    CwNullTransport transport_;
    std::unique_ptr<rep::PartitionReplica>       replica_;
    std::unique_ptr<tx::ReplicatedTxnStateStore> store_;
    std::unique_ptr<rune::node::TransactionCoordinationService> svc_;
    Answers answers_;
    std::vector<SentVote> votes_;
    int markerAppends_ = 0;
    int64_t appendResult_ = 0;   // negative: the partition takes no marker
    rune::coordination::NsRequestContext ctx_{};
    bool    voteAccepted_  = true;
    bool    cwIsCandidate_ = true;
    bool    isLeader_      = true;
    int32_t clientVersion_ = pc::kClientVersion;
    int64_t now_  = 1'000'000'000;
    int64_t corr_ = 0;
    std::array<uint8_t, 1024> reqBuf_{};
    std::array<uint8_t, 1024> outBuf_{};
};

class CheckedWritesTxnCoordinatorRegression : public CheckedWritesTxnCoordinatorTest {};

namespace {
const std::vector<pc::TxnTopicPartition> kCw0Cw1Plain0 = {{kCw, 0}, {kCw, 1}, {kPlain, 0}};
}

TEST_F(CheckedWritesTxnCoordinatorTest, CommitWithACandidateSendsVotesAndPersistsNothing) {
    int16_t ep = 0;
    const int64_t pid = beginTxn(kCw0Cw1Plain0, ep);
    const int64_t leo = replica_->logEndOffset();
    EXPECT_EQ(endTxn("tx", pid, ep, true), 0u) << "the answer is deferred";
    ASSERT_EQ(votes_.size(), 2u) << "one vote per candidate partition, none for plain-0";
    EXPECT_EQ(votes_[0].topic, "cw");
    EXPECT_EQ(votes_[0].partition, 0);
    EXPECT_EQ(votes_[1].partition, 1);
    EXPECT_EQ(votes_[0].producerId, pid);
    EXPECT_EQ(votes_[0].producerEpoch, ep);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::Ongoing);
    EXPECT_EQ(replica_->logEndOffset(), leo) << "no txn-state record before the votes are in";
    EXPECT_EQ(svc_->voteGatesForTest(), 1u);
    EXPECT_TRUE(answers_.frames.empty());
}

TEST_F(CheckedWritesTxnCoordinatorTest, AllYesRunsTodaysCommitPath) {
    int16_t ep = 0;
    const int64_t pid = beginTxn(kCw0Cw1Plain0, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    const auto asked = votes_;
    vote(asked[0]);
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::Ongoing) << "one yes of two is not a decision";
    vote(asked[1]);
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareCommit);
    EXPECT_EQ(svc_->voteGatesForTest(), 0u);
    EXPECT_EQ(markerAppends_, 0) << "the markers wait for the PrepareCommit record to commit";
    followerFetches();
    (void)svc_->doWork(now_);
    EXPECT_EQ(markerAppends_, 3) << "a marker on every partition of the transaction, plain-0 too";
    EXPECT_TRUE(answers_.frames.empty()) << "commit OK means the rows are final: not before the markers landed";
    landMarkers(pid, kCw0Cw1Plain0);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::CompleteCommit);
    ASSERT_EQ(answers_.frames.size(), 1u);
    const auto a = lastAnswer();
    EXPECT_EQ(a.errorCode, 0);
    EXPECT_FALSE(a.hasVote);
}

TEST_F(CheckedWritesTxnCoordinatorTest, FirstNoAbortsAndAnswersTheVote) {
    int16_t ep = 0;
    const int64_t pid = beginTxn(kCw0Cw1Plain0, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    const auto asked = votes_;
    vote(asked[0], static_cast<uint16_t>(pc::ErrorCode::ConstraintViolation), 0x00070000, 123);
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareAbort);
    EXPECT_EQ(svc_->voteGatesForTest(), 0u);
    vote(asked[1]);   // the other partition's late yes changes nothing
    followerFetches();
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareAbort);
    EXPECT_EQ(markerAppends_, 3);
    EXPECT_TRUE(answers_.frames.empty()) << "the no is answered once the abort markers landed";
    landMarkers(pid, kCw0Cw1Plain0);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::CompleteAbort);
    ASSERT_EQ(answers_.frames.size(), 1u);
    const auto a = lastAnswer();
    EXPECT_EQ(a.errorCode, static_cast<int16_t>(pc::ErrorCode::ConstraintViolation));
    EXPECT_TRUE(a.hasVote);
    EXPECT_EQ(a.constraintId, 0x00070000u);
    EXPECT_EQ(a.failingOffset, 123);
}

TEST_F(CheckedWritesTxnCoordinatorTest, NoCandidateNoVoteNoRoundTrip) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kPlain, 0}}, ep);
    EXPECT_EQ(endTxn("tx", pid, ep, true), 0u) << "today's path: parked until the PrepareCommit record commits";
    EXPECT_TRUE(votes_.empty());
    EXPECT_EQ(svc_->voteGatesForTest(), 0u);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareCommit);
    followerFetches();
    (void)svc_->doWork(now_);
    ASSERT_EQ(answers_.frames.size(), 1u);
    EXPECT_EQ(lastAnswer().errorCode, 0);
}

TEST_F(CheckedWritesTxnCoordinatorTest, BelowLevelFiveNoVoteNoRoundTrip) {
    svc_->setMetadataFeatureLevel(rep::MetadataFeature::kCheckedWrites - 1);
    EXPECT_FALSE(svc_->clusterVotes());
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    EXPECT_EQ(endTxn("tx", pid, ep, true), 0u);
    EXPECT_TRUE(votes_.empty());
    EXPECT_EQ(svc_->voteGatesForTest(), 0u);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareCommit);
}

TEST_F(CheckedWritesTxnCoordinatorTest, CandidateAtAddPartitionsIsAskedAfterItsConstraintDropped) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}, {kPlain, 0}}, ep);
    cwIsCandidate_ = false;   // the constraint was dropped meanwhile
    EXPECT_EQ(endTxn("tx", pid, ep, true), 0u);
    ASSERT_EQ(votes_.size(), 1u) << "the partition was a candidate when it joined the transaction";
    EXPECT_EQ(votes_[0].topic, "cw");
    EXPECT_EQ(svc_->voteGatesForTest(), 1u);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, CandidateAtCommitIsAskedThoughNotAtAddPartitions) {
    cwIsCandidate_ = false;   // no view sources the topic yet
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}, {kPlain, 0}}, ep);
    cwIsCandidate_ = true;    // a view was created meanwhile: the commit must be judged
    EXPECT_EQ(endTxn("tx", pid, ep, true), 0u);
    ASSERT_EQ(votes_.size(), 1u);
    EXPECT_EQ(votes_[0].topic, "cw");
    EXPECT_EQ(svc_->voteGatesForTest(), 1u);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, VerifiedVotingPartitionIsACandidateDespiteCatalogLagRegression) {
    cwIsCandidate_ = false;   // this node's catalog has not seen the view yet
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}, {kPlain, 0}}, ep);
    ASSERT_EQ(svc_->verifyTxnPartition(pid, ep, kPlain, 0, now_, false), 0);
    ASSERT_EQ(svc_->verifyTxnPartition(pid, ep, kCw, 0, now_, true), 0) << "the partition leader reports a verdict source";
    ASSERT_EQ(svc_->verifyTxnPartition(pid, ep, kCw, 0, now_, true), 0);
    EXPECT_EQ(endTxn("tx", pid, ep, true), 0u);
    ASSERT_EQ(votes_.size(), 1u) << "one vote, to the reporting partition only, however often it verified";
    EXPECT_EQ(votes_[0].topic, "cw");
    EXPECT_EQ(votes_[0].partition, 0);
    EXPECT_EQ(svc_->voteGatesForTest(), 1u);
}

TEST_F(CheckedWritesTxnCoordinatorTest, NotLeaderVoteIsResentWithBackoff) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    const int64_t t0 = now_;
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    ASSERT_EQ(votes_.size(), 1u);
    (void)svc_->doWork(t0);
    vote(votes_[0], 0, 0, -1, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    (void)svc_->doWork(t0 + 9'000'000);
    EXPECT_EQ(votes_.size(), 1u) << "not before 10 ms";
    (void)svc_->doWork(t0 + 10'000'000);
    ASSERT_EQ(votes_.size(), 2u);
    vote(votes_[1], 0, 0, -1, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    (void)svc_->doWork(t0 + 29'000'000);
    EXPECT_EQ(votes_.size(), 2u) << "the backoff doubled: not before 20 ms after the second refusal";
    (void)svc_->doWork(t0 + 30'000'000);
    EXPECT_EQ(votes_.size(), 3u);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::Ongoing) << "a transport error is not a no";
}

TEST_F(CheckedWritesTxnCoordinatorTest, AVoteThatCouldNotBeSentIsResentWithBackoff) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    const int64_t t0 = now_;
    voteAccepted_ = false;   // no route / full ring: sendTxnVote returned -1
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    ASSERT_EQ(votes_.size(), 1u);
    (void)svc_->doWork(t0 + 9'000'000);
    EXPECT_EQ(votes_.size(), 1u);
    (void)svc_->doWork(t0 + 10'000'000);
    EXPECT_EQ(votes_.size(), 2u);
    voteAccepted_ = true;
    (void)svc_->doWork(t0 + 30'000'000);
    EXPECT_EQ(votes_.size(), 3u);
    vote(votes_[2]);
    (void)svc_->doWork(t0 + 30'000'000);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareCommit);
}

TEST_F(CheckedWritesTxnCoordinatorTest, AHeldVoteIsNotSentUntilReleasedThenGoesOutOnTheNextRetry) {
    bool hold = true;
    svc_->setVoteHoldFn([&](std::string_view, int32_t, int64_t, int16_t) { return hold; });
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    const int64_t t0 = now_;
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    (void)svc_->doWork(t0 + 20'000'000);
    EXPECT_TRUE(votes_.empty()) << "a held vote reaches neither the override nor the gateway";
    hold = false;
    (void)svc_->doWork(t0 + 60'000'000);
    ASSERT_EQ(votes_.size(), 1u);
    vote(votes_[0]);
    (void)svc_->doWork(t0 + 60'000'000);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareCommit);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, OldSessionNoVoteIsInvalidRequestThenAbortSucceedsRegression) {
    clientVersion_ = pc::kClientVersionCheckedWrites - 1;
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    vote(votes_[0], static_cast<uint16_t>(pc::ErrorCode::TxnConflict), 0, 9);
    (void)svc_->doWork(now_);
    followerFetches();
    (void)svc_->doWork(now_);
    landMarkers(pid, {{kCw, 0}});
    ASSERT_EQ(answers_.frames.size(), 1u);
    EXPECT_EQ(answers_.frames[0].size(), pc::EndTxnResponse::ENCODED_SIZE) << "no vote trailer below version 17";
    const auto a = lastAnswer();
    EXPECT_EQ(a.errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_FALSE(a.hasVote);
    const std::size_t n = endTxn("tx", pid, ep, false);
    ASSERT_GT(n, 0u);
    EXPECT_EQ(syncAnswer(n).errorCode, 0) << "the client aborts: the transaction already ended aborted";
}

TEST_F(CheckedWritesTxnCoordinatorRegression, AddPartitionsDuringTheVoteIsConcurrentRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    EXPECT_EQ(svc_->verifyTxnPartition(pid, ep, kCw, 0, now_, false), static_cast<int16_t>(pc::ErrorCode::InvalidTxnState))
        << "a gated commit takes no more batches";
    EXPECT_EQ(addPartitions("tx", pid, ep, {{kCw, 1}}), static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions))
        << "a partition joining after its votes were asked would commit without one";
    pc::AddOffsetsToTxn off;
    off.sessionId = 1; off.correlationId = ++corr_; off.producerId = pid; off.producerEpoch = ep;
    const std::string txn = "tx", group = "g";
    off.transactionalId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(txn.data()), txn.size());
    off.groupId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(group.data()), group.size());
    const std::size_t n = off.encode(reqBuf_.data());
    const std::size_t sync = svc_->handleAddOffsets(reqBuf_.data(), n, now_, outBuf_.data(), outBuf_.size());
    ASSERT_GT(sync, 0u);
    const auto offAnswer = pc::AddOffsetsToTxnResponse::decode(outBuf_.data(), sync);
    ASSERT_TRUE(offAnswer);
    EXPECT_EQ(offAnswer->errorCode, static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions));
    EXPECT_EQ(endTxn("tx", pid, ep, true), 0u) << "a repeated commit takes the gate over";
    ASSERT_EQ(answers_.frames.size(), 1u);
    EXPECT_EQ(lastAnswer().errorCode, static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions)) << "the earlier request is told to retry";
    EXPECT_EQ(votes_.size(), 1u) << "no second round of votes";
    EXPECT_EQ(svc_->voteGatesForTest(), 1u);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, UnloadAnswersAVoteGateAndACommitWaitingForMarkersRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    isLeader_ = false;   // the coordinator moves with the votes out
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->voteGatesForTest(), 0u);
    ASSERT_EQ(answers_.frames.size(), 1u);
    EXPECT_EQ(lastAnswer().errorCode, static_cast<int16_t>(pc::ErrorCode::NotCoordinator));

    answers_.frames.clear();
    isLeader_ = true;
    followerFetches();
    (void)svc_->doWork(now_);
    ASSERT_TRUE(svc_->isLoaded());
    followerFetches();
    (void)svc_->doWork(now_);
    ASSERT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::Ongoing);
    // A second transaction, voted and decided, waits for its markers.
    int16_t ep2 = 0;
    const int64_t pid2 = initPid("tx2", ep2);
    ASSERT_EQ(addPartitions("tx2", pid2, ep2, {{kCw, 1}}), 0);
    votes_.clear();
    ASSERT_EQ(endTxn("tx2", pid2, ep2, true), 0u);
    vote(votes_[0]);
    (void)svc_->doWork(now_);
    followerFetches();
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->awaitingMarkerTxnsForTest(), 1u);
    EXPECT_TRUE(answers_.frames.empty());
    isLeader_ = false;
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->awaitingMarkerTxnsForTest(), 0u);
    ASSERT_EQ(answers_.frames.size(), 1u) << "never a hang: the client re-discovers the coordinator";
    EXPECT_EQ(lastAnswer().errorCode, static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
}

TEST_F(CheckedWritesTxnCoordinatorRegression, NoAfterTimeoutAbortCompletedIsNeverCommitOkRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    const auto asked = votes_;
    now_ += 31'000'000'000;   // past the 30 s transaction timeout: the sweep aborts it with the votes still out
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareAbort);
    EXPECT_EQ(svc_->voteGatesForTest(), 0u);
    ASSERT_EQ(answers_.frames.size(), 1u) << "the gate is stale: answered at once, not at the votes";
    EXPECT_EQ(lastAnswer().errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidTxnState));
    followerFetches();
    (void)svc_->doWork(now_);
    landMarkers(pid, {{kCw, 0}});
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::CompleteAbort);
    vote(asked[0], static_cast<uint16_t>(pc::ErrorCode::ConstraintViolation), 0x70000, 5);   // the late no
    (void)svc_->doWork(now_);
    EXPECT_EQ(answers_.frames.size(), 1u) << "the late no answers nothing (and never a commit OK)";
    EXPECT_EQ(lastAnswer().errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidTxnState));
}

TEST_F(CheckedWritesTxnCoordinatorRegression, OldSessionConcurrentIsNotLeaderRegression) {
    clientVersion_ = pc::kClientVersionTxnVerify - 1;   // does not know ConcurrentTransactions (27)
    int16_t ep = 0;
    int64_t pid = beginTxn({{kPlain, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);   // plain path, parked until PrepareCommit commits
    std::size_t n = endTxn("tx", pid, ep, true);   // the markers are not out: 27 for this retry
    ASSERT_GT(n, 0u);
    EXPECT_EQ(syncAnswer(n).errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader)) << "the retriable code it knows";

    followerFetches();   // the plain commit's parked answer
    (void)svc_->doWork(now_);
    answers_.frames.clear();
    int16_t ep2 = 0;
    const int64_t pid2 = beginTxn({{kCw, 0}}, ep2, "tx2");
    ASSERT_EQ(endTxn("tx2", pid2, ep2, true), 0u);
    EXPECT_EQ(endTxn("tx2", pid2, ep2, true), 0u);   // a gated commit: the earlier request is told 27 as well
    ASSERT_EQ(answers_.frames.size(), 1u);
    EXPECT_EQ(lastAnswer().errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader));
}

TEST_F(CheckedWritesTxnCoordinatorRegression, CommitWithNoMarkerPartitionsIsAnsweredWhenTheVotesAreInRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    appendResult_ = -1;   // every partition is gone: nothing in flight, the transaction completes at dispatch
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    vote(votes_[0]);
    (void)svc_->doWork(now_);
    followerFetches();
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::CompleteCommit);
    ASSERT_EQ(answers_.frames.size(), 1u) << "the responder is stored before the dispatch completes the transaction";
    EXPECT_EQ(lastAnswer().errorCode, 0);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, TxnOffsetCommitDuringTheVoteIsConcurrentRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    const std::string txn = "tx", group = "g";
    pc::TxnOffsetCommit m;
    m.sessionId = 1; m.correlationId = ++corr_; m.producerId = pid; m.producerEpoch = ep;
    m.transactionalId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(txn.data()), txn.size());
    m.groupId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(group.data()), group.size());
    const std::vector<pc::TxnOffsetCommitEntry> entries = {{kPlain, 0, 5, {}}};
    const std::size_t len = m.encode(reqBuf_.data(), entries);
    const std::size_t n = svc_->handleTxnOffsetCommit(reqBuf_.data(), len, now_, outBuf_.data(), outBuf_.size(), ctx_);
    ASSERT_GT(n, 0u);
    const auto r = pc::TxnOffsetCommitResponse::decode(outBuf_.data(), n);
    ASSERT_TRUE(r);
    ASSERT_EQ(r->results.size(), 1u);
    EXPECT_EQ(r->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions));
}

TEST_F(CheckedWritesTxnCoordinatorRegression, AResultForAVoteNeverSentIsNotCountedRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    voteAccepted_ = false;   // the vote could not be sent: no answer is awaited
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    ASSERT_EQ(votes_.size(), 1u);
    vote(votes_[0]);   // a stray yes
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::Ongoing);
    EXPECT_EQ(svc_->voteGatesForTest(), 1u);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, CandidatesAreForgottenWhenTheTransactionCompletesRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    EXPECT_EQ(svc_->voteCandidateTxnsForTest(), 1u);
    now_ += 31'000'000'000;   // the sweep aborts it (no commit was asked)
    (void)svc_->doWork(now_);
    followerFetches();
    (void)svc_->doWork(now_);
    landMarkers(pid, {{kCw, 0}});
    ASSERT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::CompleteAbort);
    EXPECT_EQ(svc_->voteCandidateTxnsForTest(), 0u);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, AProducerThatIsNotTheGatedOneIsAnsweredByTheCoordinatorRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    const int16_t zombie = addPartitions("tx", pid + 1000, ep, {{kCw, 1}});
    EXPECT_EQ(zombie, static_cast<int16_t>(pc::ErrorCode::ProducerFenced)) << "fenced, not retriable";
    const std::size_t n = endTxn("tx", pid, static_cast<int16_t>(ep + 1), true);
    ASSERT_GT(n, 0u);
    EXPECT_EQ(syncAnswer(n).errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidProducerEpoch));
    EXPECT_EQ(svc_->voteGatesForTest(), 1u) << "the gate is untouched";
}

TEST_F(CheckedWritesTxnCoordinatorRegression, VoteCandidatesAndGatesOfANamespaceAreCountedInMemoryRegression) {
    ctx_.sessionNsId = kNs;
    int16_t ep = 0;
    (void)beginTxn({{kPlainNs, 0}}, ep, "a");
    const std::size_t n1 = svc_->inMemoryEntriesOfNamespaceForTest(kNs);
    int16_t epB = 0;
    const int64_t pidB = beginTxn({{kCwNs, 0}}, epB, "b");
    const std::size_t n2 = svc_->inMemoryEntriesOfNamespaceForTest(kNs);
    int16_t epC = 0;
    (void)beginTxn({{kPlainNs, 1}}, epC, "c");
    const std::size_t n3 = svc_->inMemoryEntriesOfNamespaceForTest(kNs);
    EXPECT_EQ(n2 - n1, n3 - n2 + 1) << "the candidate of b is an entry of the namespace";
    ASSERT_EQ(endTxn("b", pidB, epB, true), 0u);
    ASSERT_EQ(svc_->voteGatesForTest(), 1u);
    EXPECT_EQ(svc_->inMemoryEntriesOfNamespaceForTest(kNs), n3) << "the candidate became a gate: still one entry";
}

// ---- Task 10b: races and recovery ----

TEST_F(CheckedWritesTxnCoordinatorTest, TimeoutDuringTheVoteAbortsAndAnswersInvalidTxnState) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    now_ += 31'000'000'000;   // past the 30 s transaction timeout, the votes still out
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareAbort);
    EXPECT_EQ(svc_->voteGatesForTest(), 0u);
    ASSERT_EQ(answers_.frames.size(), 1u);
    EXPECT_EQ(lastAnswer().errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidTxnState));
    EXPECT_EQ(markerAppends_, 0) << "the abort markers wait for the abort record";
    completeWithMarkers(pid, {{kCw, 0}});
    EXPECT_EQ(markerAppends_, 1);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::CompleteAbort);
    EXPECT_EQ(answers_.frames.size(), 1u) << "answered once";
}

TEST_F(CheckedWritesTxnCoordinatorTest, RepeatedCommitTakesOverTheGate) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    const int64_t first = corr_;
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u) << "the retry waits for the same votes";
    const int64_t second = corr_;
    ASSERT_EQ(answers_.frames.size(), 1u);
    EXPECT_EQ(lastAnswer().errorCode, static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions));
    EXPECT_EQ(lastAnswer().correlationId, first);
    EXPECT_EQ(votes_.size(), 1u) << "no second round of votes";
    EXPECT_EQ(svc_->voteGatesForTest(), 1u);
    vote(votes_[0]);
    (void)svc_->doWork(now_);
    completeWithMarkers(pid, {{kCw, 0}});
    ASSERT_EQ(answers_.frames.size(), 2u);
    EXPECT_EQ(lastAnswer().errorCode, 0);
    EXPECT_EQ(lastAnswer().correlationId, second) << "the final answer goes to the request that took the gate";
}

TEST_F(CheckedWritesTxnCoordinatorTest, AbortDuringTheVoteCancelsTheGate) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    const auto asked = votes_;
    const int64_t commitCorr = corr_;
    ASSERT_EQ(endTxn("tx", pid, ep, false), 0u) << "the abort runs today's path: answered with its record";
    EXPECT_EQ(svc_->voteGatesForTest(), 0u);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareAbort);
    ASSERT_EQ(answers_.frames.size(), 1u);
    EXPECT_EQ(lastAnswer().errorCode, static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions));
    EXPECT_EQ(lastAnswer().correlationId, commitCorr);
    followerFetches();
    (void)svc_->doWork(now_);
    ASSERT_EQ(answers_.frames.size(), 2u);
    EXPECT_EQ(lastAnswer().errorCode, 0) << "the abort";
    // The cancelled vote is still out: its late yes is dropped, and the producer's next transaction would wait for it.
    EXPECT_EQ(svc_->voteOrphansForTest(), 1u);
    vote(asked[0]);
    EXPECT_EQ(svc_->voteOrphansForTest(), 0u);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareAbort);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, LateYesFromAResolvedGateIsNotCountedRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    const auto asked = votes_;
    now_ += 31'000'000'000;   // the timeout abort keeps the epoch
    (void)svc_->doWork(now_);
    completeWithMarkers(pid, {{kCw, 0}});
    ASSERT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::CompleteAbort);
    EXPECT_EQ(svc_->voteOrphansForTest(), 1u);
    answers_.frames.clear();
    ASSERT_EQ(addPartitions("tx", pid, ep, {{kCw, 0}}), 0);   // the next transaction of the same producer
    votes_.clear();
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    EXPECT_TRUE(votes_.empty()) << "one live vote per key: the old one is still out";
    EXPECT_EQ(svc_->doWork(now_), 0) << "waiting for the old answer is not work";
    vote(asked[0]);   // the old gate's late yes
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::Ongoing) << "a yes the new transaction never asked for";
    EXPECT_EQ(svc_->voteGatesForTest(), 1u);
    ASSERT_EQ(votes_.size(), 1u);
    vote(votes_[0]);
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareCommit);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, LateNoFromAGateResolvedByAFirstNoIsNotCountedRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}, {kCw, 1}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    const auto asked = votes_;
    ASSERT_EQ(asked.size(), 2u);
    vote(asked[0], static_cast<uint16_t>(pc::ErrorCode::ConstraintViolation), 0x00070000, 123);
    (void)svc_->doWork(now_);   // the first no: cw-1's vote is still out
    completeWithMarkers(pid, {{kCw, 0}, {kCw, 1}});
    ASSERT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::CompleteAbort);
    EXPECT_EQ(svc_->voteOrphansForTest(), 1u) << "cw-1's vote";
    answers_.frames.clear();
    ASSERT_EQ(addPartitions("tx", pid, ep, {{kCw, 1}}), 0);
    votes_.clear();
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    EXPECT_TRUE(votes_.empty());
    vote(asked[1], static_cast<uint16_t>(pc::ErrorCode::ConstraintViolation), 0x00070001, 9);   // the old gate's late no
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::Ongoing) << "the new transaction is not aborted by it";
    ASSERT_EQ(votes_.size(), 1u);
    vote(votes_[0]);
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareCommit);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, FeatureLevelBumpedMidTransactionVotesEveryPartitionRegression) {
    svc_->setMetadataFeatureLevel(rep::MetadataFeature::kCheckedWrites - 1);
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}, {kPlain, 0}}, ep);   // started while the cluster could not vote
    svc_->setMetadataFeatureLevel(rep::MetadataFeature::kCheckedWrites);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    ASSERT_EQ(votes_.size(), 2u) << "never a constrained write committed unvoted: every partition of the transaction votes";
    vote(votes_[0]);
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::Ongoing);
    vote(votes_[1]);
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareCommit);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, FailoverBetweenAddPartitionsAndCommitCollectsEveryVoteRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}, {kPlain, 0}}, ep);
    isLeader_ = false;
    (void)svc_->doWork(now_);   // unload
    isLeader_ = true;
    followerFetches();
    (void)svc_->doWork(now_);   // reload from the persisted state only
    followerFetches();
    (void)svc_->doWork(now_);
    ASSERT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::Ongoing);
    cwIsCandidate_ = false;     // the constraint was dropped meanwhile: the persisted set still votes
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    ASSERT_EQ(votes_.size(), 2u) << "one vote per partition of the persisted set";
    vote(votes_[1]);            // plain-0 says yes
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::Ongoing) << "the gate resolves only after both answers";
    vote(votes_[0], static_cast<uint16_t>(pc::ErrorCode::ConstraintViolation), 0x00070000, 123);
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareAbort);
    completeWithMarkers(pid, {{kCw, 0}, {kPlain, 0}});
    ASSERT_FALSE(answers_.frames.empty());
    const auto a = lastAnswer();
    EXPECT_EQ(a.errorCode, static_cast<int16_t>(pc::ErrorCode::ConstraintViolation));
    EXPECT_TRUE(a.hasVote);
    EXPECT_EQ(a.failingOffset, 123);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, UnloadWithVotesOutThenRetryGetsTheSameAnswerRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    const auto asked = votes_;
    isLeader_ = false;
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->voteGatesForTest(), 0u);
    ASSERT_EQ(answers_.frames.size(), 1u);
    EXPECT_EQ(lastAnswer().errorCode, static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
    answers_.frames.clear();
    isLeader_ = true;
    followerFetches();
    (void)svc_->doWork(now_);
    followerFetches();
    (void)svc_->doWork(now_);
    ASSERT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::Ongoing);
    votes_.clear();
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u) << "the client's retry";
    ASSERT_EQ(votes_.size(), 1u) << "the votes are asked again: the old vote died with the old load";
    vote(votes_[0], static_cast<uint16_t>(pc::ErrorCode::ConstraintViolation), 0x00070000, 123);
    (void)svc_->doWork(now_);
    completeWithMarkers(pid, {{kCw, 0}});
    ASSERT_EQ(answers_.frames.size(), 1u);
    const auto a = lastAnswer();
    EXPECT_EQ(a.errorCode, static_cast<int16_t>(pc::ErrorCode::ConstraintViolation));
    EXPECT_TRUE(a.hasVote);
    EXPECT_EQ(a.constraintId, 0x00070000u);
    EXPECT_EQ(a.failingOffset, 123);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, OrphanedVotesAreForgottenAtUnloadRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    now_ += 31'000'000'000;   // the timeout orphans the vote
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->voteOrphansForTest(), 1u);
    isLeader_ = false;        // the gateway purges its votes with the old load
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->voteOrphansForTest(), 0u);
    isLeader_ = true;
    followerFetches();
    (void)svc_->doWork(now_);
    followerFetches();
    (void)svc_->doWork(now_);
    ASSERT_TRUE(svc_->isLoaded());
    ASSERT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareAbort) << "the reload re-drives the persisted abort";
    completeWithMarkers(pid, {{kCw, 0}});
    answers_.frames.clear();
    ASSERT_EQ(addPartitions("tx", pid, ep, {{kCw, 0}}), 0);
    votes_.clear();
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    EXPECT_EQ(votes_.size(), 1u) << "no answer of the old load will ever come: nothing to wait for";
}

TEST_F(CheckedWritesTxnCoordinatorTest, RememberedNoAnswersALateRetry) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    vote(votes_[0], static_cast<uint16_t>(pc::ErrorCode::ConstraintViolation), 0x00070000, 123);
    (void)svc_->doWork(now_);
    ASSERT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareAbort);
    std::size_t n = endTxn("tx", pid, ep, true);   // the abort is still writing its markers
    ASSERT_GT(n, 0u);
    EXPECT_EQ(syncAnswer(n).errorCode, static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions));
    completeWithMarkers(pid, {{kCw, 0}});
    ASSERT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::CompleteAbort);
    answers_.frames.clear();
    n = endTxn("tx", pid, ep, true);   // the answer was lost: the client asks again
    ASSERT_GT(n, 0u);
    const auto a = syncAnswer(n);
    EXPECT_EQ(a.errorCode, static_cast<int16_t>(pc::ErrorCode::ConstraintViolation)) << "the same no, not 19";
    EXPECT_TRUE(a.hasVote);
    EXPECT_EQ(a.constraintId, 0x00070000u);
    EXPECT_EQ(a.failingOffset, 123);
    n = endTxn("tx", pid, static_cast<int16_t>(ep + 1), true);   // another epoch is the coordinator's to answer
    ASSERT_GT(n, 0u);
    EXPECT_EQ(syncAnswer(n).errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidProducerEpoch));
    ASSERT_EQ(addPartitions("tx", pid, ep, {{kPlain, 0}}), 0);   // a new transaction: the old no is history
    now_ += 31'000'000'000;   // it times out and aborts
    (void)svc_->doWork(now_);
    completeWithMarkers(pid, {{kPlain, 0}});
    ASSERT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::CompleteAbort);
    n = endTxn("tx", pid, ep, true);
    ASSERT_GT(n, 0u);
    const auto b = syncAnswer(n);
    EXPECT_EQ(b.errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidTxnState)) << "not the previous transaction's no";
    EXPECT_FALSE(b.hasVote);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, RememberedNoBelowVersionSeventeenHasNoTrailerRegression) {
    clientVersion_ = pc::kClientVersionCheckedWrites - 1;
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    vote(votes_[0], static_cast<uint16_t>(pc::ErrorCode::TxnConflict), 0, 9);
    (void)svc_->doWork(now_);
    completeWithMarkers(pid, {{kCw, 0}});
    const std::size_t n = endTxn("tx", pid, ep, true);
    ASSERT_GT(n, 0u);
    EXPECT_EQ(n, pc::EndTxnResponse::ENCODED_SIZE);
    const auto a = syncAnswer(n);
    EXPECT_EQ(a.errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_FALSE(a.hasVote);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, RememberedNoOfANamespaceIsForgottenWithItRegression) {
    ctx_.sessionNsId = kNs;
    int16_t epA = 0;
    const int64_t pidA = beginTxn({{kCwNs, 0}}, epA, "a");
    ASSERT_EQ(endTxn("a", pidA, epA, true), 0u);
    rune::gateway::TxnVoteResult yes, no;
    no.code = static_cast<uint16_t>(pc::ErrorCode::ConstraintViolation);
    svc_->onTxnVoteResult(kCwNs, 0, pidA, epA, svc_->coordinatorEpoch(), yes);
    (void)svc_->doWork(now_);
    completeWithMarkers(pidA, {{kCwNs, 0}});
    ASSERT_EQ(answers_.frames.size(), 1u);
    const std::size_t afterYes = svc_->inMemoryEntriesOfNamespaceForTest(kNs);
    answers_.frames.clear();
    int16_t epB = 0;
    const int64_t pidB = beginTxn({{kCwNs, 0}}, epB, "b");
    ASSERT_EQ(endTxn("b", pidB, epB, true), 0u);
    svc_->onTxnVoteResult(kCwNs, 0, pidB, epB, svc_->coordinatorEpoch(), no);
    (void)svc_->doWork(now_);
    completeWithMarkers(pidB, {{kCwNs, 0}});
    ASSERT_EQ(answers_.frames.size(), 1u) << "the no was answered: the transaction is complete";
    EXPECT_EQ(svc_->inMemoryEntriesOfNamespaceForTest(kNs), 2 * afterYes + 1) << "b has the entries of a, plus its remembered no";
    for (int i = 0; i < 8 && svc_->tombstoneNamespace(kNs, 64, now_) != rune::node::ReapStatus::Done; ++i) {
        followerFetches();
        (void)svc_->doWork(now_);
    }
    EXPECT_EQ(svc_->inMemoryEntriesOfNamespaceForTest(kNs), 0u);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, AddOffsetsOnlyNextTransactionForgetsTheRememberedNoRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    vote(votes_[0], static_cast<uint16_t>(pc::ErrorCode::ConstraintViolation), 0x00070000, 123);
    (void)svc_->doWork(now_);
    completeWithMarkers(pid, {{kCw, 0}});
    answers_.frames.clear();
    pc::AddOffsetsToTxn off;   // the next transaction opens with offsets only (consume-transform)
    off.sessionId = 1; off.correlationId = ++corr_; off.producerId = pid; off.producerEpoch = ep;
    const std::string txn = "tx", group = "g";
    off.transactionalId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(txn.data()), txn.size());
    off.groupId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(group.data()), group.size());
    std::size_t n = svc_->handleAddOffsets(reqBuf_.data(), off.encode(reqBuf_.data()), now_, outBuf_.data(), outBuf_.size());
    if (n == 0) { followerFetches(); (void)svc_->doWork(now_); }
    ASSERT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::Ongoing);
    now_ += 31'000'000'000;   // it times out and aborts
    (void)svc_->doWork(now_);
    completeWithMarkers(pid, {});
    ASSERT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::CompleteAbort);
    n = endTxn("tx", pid, ep, true);
    ASSERT_GT(n, 0u);
    const auto a = syncAnswer(n);
    EXPECT_EQ(a.errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidTxnState)) << "not the previous transaction's no";
    EXPECT_FALSE(a.hasVote);
}

TEST_F(CheckedWritesTxnCoordinatorRegression, AllYesAndTheTimeoutInOnePassIsNeverACommitRegression) {
    int16_t ep = 0;
    const int64_t pid = beginTxn({{kCw, 0}}, ep);
    ASSERT_EQ(endTxn("tx", pid, ep, true), 0u);
    vote(votes_[0]);          // the last yes is in ...
    now_ += 31'000'000'000;   // ... and so is the deadline: the sweep (first in doWork) decides before the gate
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("tx"), tx::TxnState::PrepareAbort);
    ASSERT_EQ(answers_.frames.size(), 1u);
    EXPECT_EQ(lastAnswer().errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidTxnState));
}
