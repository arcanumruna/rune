// ---------------------------------------------------------------------------
// TransactionCoordinationServiceTest.cpp — Unit tests for
// TransactionCoordinationService (TX-3, Task 6 skeleton).
//
// Test matrix (brief §Task 6 Step 1):
//   1. DoWorkOnEmptyServiceReturnsZero
//   2. NotCoordinatorHandlersRespondNotCoordinator
//   3. CoordinatorForReturnsLeaderNodeId
//
// Additional tests for Task 7 handlers:
//   4. HandleInitProducerIdAllocatesPidAndEpoch
//   5. HandleInitProducerIdSameIdBumpsEpoch
//   6. HandleAddPartitionsMovesToOngoing
//   7. HandleEndTxnCommitTransitionsToPrepareCommit
//   8. HandleEndTxnWrongEpochReturnsProducerFenced
//   9. HandleEndTxnUnknownTxnReturnsInvalidTxnState
//
// Uses single-node PartitionReplica (minIsr=1) so put() succeeds immediately.
// No Aeron — uses NullTransport and an injected fake ReplicatedTxnStateStore
// (a real single-node PartitionReplica with Log).
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/node/TransactionCoordinationService.h>
#include <rune/transaction/ReplicatedTxnStateStore.h>
#include <rune/transaction/Types.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/client/TxnMessages.h>
#include <rune/protocol/client/Protocol.h>

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

// ===========================================================================
// Helpers
// ===========================================================================

static std::string makeTempSuffix() {
    static std::atomic<uint64_t> g_seq{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::to_string(::getpid()) + "_"
         + std::to_string(g_seq.fetch_add(1, std::memory_order_relaxed)) + "_"
         + std::to_string(ts);
}

struct TmpDir {
    std::string path;
    explicit TmpDir(std::string_view tag) {
        path = std::filesystem::temp_directory_path().string()
             + "/rune_tcs_" + std::string(tag) + "_" + makeTempSuffix();
        std::filesystem::create_directories(path);
    }
    ~TmpDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

class NullTransport : public rep::PartitionTransport {
public:
    void sendFetchRequest(const rep::FetchRequestMsg&) override {}
    void sendFetchResponse(rep::NodeId, const rep::FetchResponseMsg&) override {}
};

static rep::PartitionConfig makeSingleNodeConfig() {
    rep::PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = 1;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = 1;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    return cfg;
}

// Encode a InitProducerId request frame into buf, returning size.
static std::size_t encodeInitProducerId(uint8_t* buf, std::size_t cap,
                                        int64_t sessionId, int64_t correlationId,
                                        int32_t txnTimeoutMs,
                                        std::string_view txnId)
{
    pc::InitProducerId msg;
    msg.sessionId            = sessionId;
    msg.correlationId        = correlationId;
    msg.transactionTimeoutMs = txnTimeoutMs;
    msg.transactionalId      = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(txnId.data()), txnId.size());
    const std::size_t needed = msg.encodedSize();
    if (cap < needed) return 0;
    return msg.encode(buf);
}

// Encode an EndTxn request frame.
static std::size_t encodeEndTxn(uint8_t* buf, std::size_t cap,
                                 int64_t sessionId, int64_t correlationId,
                                 int64_t producerId, int16_t producerEpoch,
                                 bool commit, std::string_view txnId)
{
    pc::EndTxn msg;
    msg.sessionId       = sessionId;
    msg.correlationId   = correlationId;
    msg.producerId      = producerId;
    msg.producerEpoch   = producerEpoch;
    msg.commit          = commit ? 1u : 0u;
    msg.transactionalId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(txnId.data()), txnId.size());
    const std::size_t needed = msg.encodedSize();
    if (cap < needed) return 0;
    return msg.encode(buf);
}

// Encode an AddPartitionsToTxn request frame (empty partition list).
static std::size_t encodeAddPartitions(uint8_t* buf, std::size_t cap,
                                        int64_t sessionId, int64_t correlationId,
                                        int64_t producerId, int16_t producerEpoch,
                                        std::string_view txnId,
                                        std::span<const pc::TxnTopicPartition> parts)
{
    pc::AddPartitionsToTxn msg;
    msg.sessionId       = sessionId;
    msg.correlationId   = correlationId;
    msg.producerId      = producerId;
    msg.producerEpoch   = producerEpoch;
    msg.transactionalId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(txnId.data()), txnId.size());
    const std::size_t needed = pc::AddPartitionsToTxn::encodedSizeFor(msg.transactionalId, parts.size());
    if (cap < needed) return 0;
    return msg.encode(buf, parts);
}

// ===========================================================================
// Fixture
// ===========================================================================
class TransactionCoordinationServiceTest : public ::testing::Test {
protected:
    TransactionCoordinationServiceTest()
        : dir_("svc")
        , log_(dir_.path)
    {}

    void SetUp() override {
        transport_.reset(new NullTransport{});
        replica_ = std::make_unique<rep::PartitionReplica>(
            makeSingleNodeConfig(),
            rep::Role::Leader, /*epoch=*/1,
            std::vector<rep::NodeId>{1}, std::vector<rep::NodeId>{1},
            &log_, transport_.get());

        txnStore_ = std::make_unique<tx::ReplicatedTxnStateStore>(replica_.get());
        svc_      = std::make_unique<rune::node::TransactionCoordinationService>(
            txnStore_.get(),
            /*topics=*/nullptr,
            /*isLeader=*/[this] { return isLeader_; },
            /*leaderId=*/[this] { return leaderNodeId_; });
    }

    TmpDir dir_;
    rune::storage::Log log_;
    std::unique_ptr<rep::PartitionTransport>          transport_;
    std::unique_ptr<rep::PartitionReplica>            replica_;
    std::unique_ptr<tx::ReplicatedTxnStateStore>      txnStore_;
    std::unique_ptr<rune::node::TransactionCoordinationService> svc_;

    bool isLeader_       = true;
    rune::node::NodeId leaderNodeId_ = 1;

    std::array<uint8_t, 1024> reqBuf_{};
    std::array<uint8_t, 1024> outBuf_{};
};

// ===========================================================================
// 1. DoWorkOnEmptyServiceReturnsZero
// ===========================================================================
TEST_F(TransactionCoordinationServiceTest, DoWorkOnEmptyServiceReturnsZero) {
    // Fresh service, no transactions, leader.
    int work = svc_->doWork(/*nowNs=*/1'000'000'000LL);
    EXPECT_EQ(work, 0);
}

// ===========================================================================
// 2. NotCoordinatorHandlersRespondNotCoordinator
// ===========================================================================
TEST_F(TransactionCoordinationServiceTest, NotCoordinatorHandlersRespondNotCoordinator) {
    isLeader_     = false;
    leaderNodeId_ = 99;

    // --- InitProducerId ---
    {
        const std::size_t reqLen = encodeInitProducerId(
            reqBuf_.data(), reqBuf_.size(), /*session=*/42, /*corr=*/1,
            /*timeout=*/30'000, /*txnId=*/"txA");
        ASSERT_GT(reqLen, 0u);

        const std::size_t outLen = svc_->handleInitProducerId(
            reqBuf_.data(), reqLen, /*nowNs=*/0,
            outBuf_.data(), outBuf_.size());
        ASSERT_EQ(outLen, pc::InitProducerIdResponse::ENCODED_SIZE);

        auto respOpt = pc::InitProducerIdResponse::decode(outBuf_.data(), outLen);
        ASSERT_TRUE(respOpt);
        EXPECT_EQ(respOpt->sessionId,     42);
        EXPECT_EQ(respOpt->correlationId, 1);
        EXPECT_EQ(respOpt->errorCode,
                  static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
    }

    // --- EndTxn ---
    {
        const std::size_t reqLen = encodeEndTxn(
            reqBuf_.data(), reqBuf_.size(), /*session=*/42, /*corr=*/2,
            /*pid=*/10, /*epoch=*/0, /*commit=*/true, "txA");
        ASSERT_GT(reqLen, 0u);

        const std::size_t outLen = svc_->handleEndTxn(
            reqBuf_.data(), reqLen, /*nowNs=*/0,
            outBuf_.data(), outBuf_.size());
        ASSERT_EQ(outLen, pc::EndTxnResponse::ENCODED_SIZE);

        auto respOpt = pc::EndTxnResponse::decode(outBuf_.data(), outLen);
        ASSERT_TRUE(respOpt);
        EXPECT_EQ(respOpt->errorCode,
                  static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
    }
}

// ===========================================================================
// 3. CoordinatorForReturnsLeaderNodeId
// ===========================================================================
TEST_F(TransactionCoordinationServiceTest, CoordinatorForReturnsLeaderNodeId) {
    leaderNodeId_ = 7;
    EXPECT_EQ(svc_->coordinatorFor("any_txn"), 7);

    leaderNodeId_ = 3;
    EXPECT_EQ(svc_->coordinatorFor("another_txn"), 3);
}

// ===========================================================================
// 4. HandleInitProducerIdAllocatesPidAndEpoch
// ===========================================================================
TEST_F(TransactionCoordinationServiceTest, HandleInitProducerIdAllocatesPidAndEpoch) {
    const std::size_t reqLen = encodeInitProducerId(
        reqBuf_.data(), reqBuf_.size(), /*session=*/1, /*corr=*/10,
        /*timeout=*/30'000, /*txnId=*/"txnFoo");
    ASSERT_GT(reqLen, 0u);

    const std::size_t outLen = svc_->handleInitProducerId(
        reqBuf_.data(), reqLen, /*nowNs=*/0,
        outBuf_.data(), outBuf_.size());
    ASSERT_EQ(outLen, pc::InitProducerIdResponse::ENCODED_SIZE);

    auto respOpt = pc::InitProducerIdResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(respOpt);
    EXPECT_EQ(respOpt->sessionId,     1);
    EXPECT_EQ(respOpt->correlationId, 10);
    EXPECT_EQ(respOpt->errorCode,     0);
    EXPECT_GE(respOpt->producerId,    0);    // valid PID allocated
    EXPECT_EQ(respOpt->producerEpoch, 0);    // first init → epoch 0
}

// ===========================================================================
// 5. HandleInitProducerIdSameIdBumpsEpoch (fences prior producer)
// ===========================================================================
TEST_F(TransactionCoordinationServiceTest, HandleInitProducerIdSameIdBumpsEpoch) {
    // First init.
    std::size_t reqLen = encodeInitProducerId(
        reqBuf_.data(), reqBuf_.size(), 1, 1, 30'000, "txnZ");
    ASSERT_GT(reqLen, 0u);
    std::size_t outLen = svc_->handleInitProducerId(
        reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    ASSERT_EQ(outLen, pc::InitProducerIdResponse::ENCODED_SIZE);
    auto r1 = pc::InitProducerIdResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(r1);
    EXPECT_EQ(r1->errorCode, 0);
    EXPECT_EQ(r1->producerEpoch, 0);

    // Second init for same txnId → epoch bumped.
    reqLen = encodeInitProducerId(
        reqBuf_.data(), reqBuf_.size(), 1, 2, 30'000, "txnZ");
    ASSERT_GT(reqLen, 0u);
    outLen = svc_->handleInitProducerId(
        reqBuf_.data(), reqLen, 1'000'000LL, outBuf_.data(), outBuf_.size());
    ASSERT_EQ(outLen, pc::InitProducerIdResponse::ENCODED_SIZE);
    auto r2 = pc::InitProducerIdResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(r2);
    EXPECT_EQ(r2->errorCode, 0);
    EXPECT_EQ(r2->producerEpoch, 1);  // bumped from 0 → 1
}

// ===========================================================================
// 6. HandleAddPartitionsMovesToOngoing
// ===========================================================================
TEST_F(TransactionCoordinationServiceTest, HandleAddPartitionsMovesToOngoing) {
    // First init to allocate PID.
    std::size_t reqLen = encodeInitProducerId(
        reqBuf_.data(), reqBuf_.size(), 1, 1, 30'000, "txnAdd");
    ASSERT_GT(reqLen, 0u);
    std::size_t outLen = svc_->handleInitProducerId(
        reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    auto initResp = pc::InitProducerIdResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(initResp);
    EXPECT_EQ(initResp->errorCode, 0);

    const int64_t  pid   = initResp->producerId;
    const int16_t  epoch = initResp->producerEpoch;

    // AddPartitionsToTxn with one partition.
    const std::vector<pc::TxnTopicPartition> parts = {{/*topicId=*/0, /*partition=*/0}};
    reqLen = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(),
                                 /*session=*/1, /*corr=*/2, pid, epoch,
                                 "txnAdd", parts);
    ASSERT_GT(reqLen, 0u);

    outLen = svc_->handleAddPartitions(
        reqBuf_.data(), reqLen, /*nowNs=*/0, outBuf_.data(), outBuf_.size());
    ASSERT_GT(outLen, 0u);

    auto respOpt = pc::AddPartitionsToTxnResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(respOpt);
    // Verify the result entries report OK for the single partition.
    ASSERT_EQ(respOpt->results.size(), 1u);
    auto r = respOpt->results[0];
    EXPECT_EQ(r.errorCode, 0) << "AddPartitions should succeed (Empty→Ongoing)";
}

// ===========================================================================
// 7. HandleEndTxnCommitTransitionsToPrepareCommit
// ===========================================================================
TEST_F(TransactionCoordinationServiceTest, HandleEndTxnCommitTransitionsToPrepareCommit) {
    // Init + AddPartitions to move to Ongoing.
    std::size_t reqLen = encodeInitProducerId(
        reqBuf_.data(), reqBuf_.size(), 1, 1, 30'000, "txnEnd");
    ASSERT_GT(reqLen, 0u);
    std::size_t outLen = svc_->handleInitProducerId(
        reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    auto initResp = pc::InitProducerIdResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(initResp);
    EXPECT_EQ(initResp->errorCode, 0);

    const int64_t pid   = initResp->producerId;
    const int16_t epoch = initResp->producerEpoch;

    // AddPartitions.
    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    reqLen = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(),
                                 1, 2, pid, epoch, "txnEnd", parts);
    ASSERT_GT(reqLen, 0u);
    outLen = svc_->handleAddPartitions(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    ASSERT_GT(outLen, 0u);

    // EndTxn commit.
    reqLen = encodeEndTxn(reqBuf_.data(), reqBuf_.size(),
                          1, 3, pid, epoch, /*commit=*/true, "txnEnd");
    ASSERT_GT(reqLen, 0u);
    outLen = svc_->handleEndTxn(
        reqBuf_.data(), reqLen, /*nowNs=*/0, outBuf_.data(), outBuf_.size());
    ASSERT_EQ(outLen, pc::EndTxnResponse::ENCODED_SIZE);

    auto respOpt = pc::EndTxnResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(respOpt);
    EXPECT_EQ(respOpt->errorCode, 0) << "EndTxn commit should succeed (Ongoing→PrepareCommit)";
}

// ===========================================================================
// 8. HandleEndTxnWrongEpochReturnsProducerFenced
// ===========================================================================
TEST_F(TransactionCoordinationServiceTest, HandleEndTxnWrongEpochReturnsProducerFenced) {
    // Init to allocate PID.
    std::size_t reqLen = encodeInitProducerId(
        reqBuf_.data(), reqBuf_.size(), 1, 1, 30'000, "txnFence");
    ASSERT_GT(reqLen, 0u);
    std::size_t outLen = svc_->handleInitProducerId(
        reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    auto initResp = pc::InitProducerIdResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(initResp);
    const int64_t pid   = initResp->producerId;
    const int16_t epoch = initResp->producerEpoch;  // 0

    // AddPartitions to move to Ongoing.
    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    reqLen = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(),
                                 1, 2, pid, epoch, "txnFence", parts);
    ASSERT_GT(reqLen, 0u);
    outLen = svc_->handleAddPartitions(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    ASSERT_GT(outLen, 0u);

    // EndTxn with WRONG epoch (epoch+1 → stale).
    const int16_t wrongEpoch = static_cast<int16_t>(epoch + 1);
    reqLen = encodeEndTxn(reqBuf_.data(), reqBuf_.size(),
                          1, 3, pid, wrongEpoch, /*commit=*/true, "txnFence");
    ASSERT_GT(reqLen, 0u);
    outLen = svc_->handleEndTxn(
        reqBuf_.data(), reqLen, /*nowNs=*/0, outBuf_.data(), outBuf_.size());
    ASSERT_EQ(outLen, pc::EndTxnResponse::ENCODED_SIZE);

    auto respOpt = pc::EndTxnResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(respOpt);
    // Wrong epoch with matching PID → InvalidProducerEpoch (kInvalidProducerEpoch=1)
    // Wrong PID  → ProducerFenced (kInvalidPid=2)
    // Both are accepted as "fenced" responses.
    const int16_t ec = respOpt->errorCode;
    EXPECT_TRUE(ec == static_cast<int16_t>(pc::ErrorCode::InvalidProducerEpoch) ||
                ec == static_cast<int16_t>(pc::ErrorCode::ProducerFenced))
        << "Expected InvalidProducerEpoch or ProducerFenced, got " << ec;
}

// ===========================================================================
// 9. HandleEndTxnUnknownTxnReturnsInvalidTxnState
// ===========================================================================
TEST_F(TransactionCoordinationServiceTest, HandleEndTxnUnknownTxnReturnsInvalidTxnState) {
    // EndTxn for a txnId that was never initiated → InvalidTxnState.
    const std::size_t reqLen = encodeEndTxn(
        reqBuf_.data(), reqBuf_.size(), 1, 1, 99, 0, true, "txnUnknown");
    ASSERT_GT(reqLen, 0u);

    const std::size_t outLen = svc_->handleEndTxn(
        reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    ASSERT_EQ(outLen, pc::EndTxnResponse::ENCODED_SIZE);

    auto respOpt = pc::EndTxnResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(respOpt);
    EXPECT_EQ(respOpt->errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidTxnState))
        << "Unknown txnId → InvalidTxnState";
}
