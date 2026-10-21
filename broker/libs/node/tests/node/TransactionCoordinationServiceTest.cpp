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
#include <rune/protocol/ProducerIdBinding.h>

#include <rune/node/TransactionCoordinationService.h>
#include <rune/transaction/ReplicatedTxnStateStore.h>
#include <rune/transaction/Types.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/protocol/client/TxnMessages.h>
#include <rune/gateway/ITxnRequestHandler.h>
#include <rune/protocol/client/MessageHeader.h>
#include <array>
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
    bool sendFetchRequest(const rep::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rep::NodeId, const rep::FetchResponseMsg&) override { return true; }
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

// ===========================================================================
// 10. HandleInitProducerIdPersistsTxnState (durability: put() is called)
// ===========================================================================
TEST_F(TransactionCoordinationServiceTest, HandleInitProducerIdPersistsTxnState) {
    const std::size_t reqLen = encodeInitProducerId(
        reqBuf_.data(), reqBuf_.size(), 1, 1, 30'000, "txnPersist");
    ASSERT_GT(reqLen, 0u);
    ASSERT_EQ(svc_->handleInitProducerId(
        reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size()),
        pc::InitProducerIdResponse::ENCODED_SIZE);

    // The txnStateStore_ should have the metadata (put was called).
    const auto* meta = txnStore_->get("txnPersist");
    ASSERT_TRUE(meta) << "handleInitProducerId must persist metadata via ReplicatedTxnStateStore";
    EXPECT_EQ(meta->txnId, "txnPersist");
    EXPECT_GE(meta->producerId, 0);
    EXPECT_EQ(meta->producerEpoch, 0);
}

// ===========================================================================
// 11. HandleEndTxnPrepareCommitIsDurable
// ===========================================================================
TEST_F(TransactionCoordinationServiceTest, HandleEndTxnPrepareCommitIsDurable) {
    // Init + AddPartitions + EndTxn(commit) → verify PrepareCommit state is in store.
    std::size_t reqLen = encodeInitProducerId(
        reqBuf_.data(), reqBuf_.size(), 1, 1, 30'000, "txnDurable");
    ASSERT_GT(reqLen, 0u);
    std::size_t outLen = svc_->handleInitProducerId(
        reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    auto initResp = pc::InitProducerIdResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(initResp);
    const int64_t pid   = initResp->producerId;
    const int16_t epoch = initResp->producerEpoch;

    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    reqLen = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(),
                                 1, 2, pid, epoch, "txnDurable", parts);
    ASSERT_GT(reqLen, 0u);
    svc_->handleAddPartitions(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());

    reqLen = encodeEndTxn(reqBuf_.data(), reqBuf_.size(),
                          1, 3, pid, epoch, /*commit=*/true, "txnDurable");
    ASSERT_GT(reqLen, 0u);
    outLen = svc_->handleEndTxn(
        reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    auto endResp = pc::EndTxnResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(endResp);
    ASSERT_EQ(endResp->errorCode, 0);

    // State in the TxnStateStore should be PrepareCommit.
    const auto* meta = txnStore_->get("txnDurable");
    ASSERT_TRUE(meta) << "EndTxn must persist PrepareCommit state";
    EXPECT_EQ(meta->state, tx::TxnState::PrepareCommit)
        << "State must be PrepareCommit after endTxn(commit=true)";
}

// ===========================================================================
// 12. HandleAddOffsetsAndTxnOffsetCommitFlow
// ===========================================================================
TEST_F(TransactionCoordinationServiceTest, HandleAddOffsetsAndTxnOffsetCommitFlow) {
    // Init to get PID.
    std::size_t reqLen = encodeInitProducerId(
        reqBuf_.data(), reqBuf_.size(), 1, 1, 30'000, "txnOffsets");
    ASSERT_GT(reqLen, 0u);
    std::size_t outLen = svc_->handleInitProducerId(
        reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    auto initResp = pc::InitProducerIdResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(initResp);
    const int64_t pid   = initResp->producerId;
    const int16_t epoch = initResp->producerEpoch;

    // AddPartitions to move to Ongoing.
    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    reqLen = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(),
                                 1, 2, pid, epoch, "txnOffsets", parts);
    ASSERT_GT(reqLen, 0u);
    outLen = svc_->handleAddPartitions(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    auto addPartResp = pc::AddPartitionsToTxnResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(addPartResp);

    // AddOffsetsToTxn — register consumer group intent.
    pc::AddOffsetsToTxn addOffsMsg;
    addOffsMsg.sessionId       = 1;
    addOffsMsg.correlationId   = 3;
    addOffsMsg.producerId      = pid;
    addOffsMsg.producerEpoch   = epoch;
    const std::string txnIdStr = "txnOffsets";
    const std::string grpStr   = "cg1";
    addOffsMsg.transactionalId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(txnIdStr.data()), txnIdStr.size());
    addOffsMsg.groupId = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(grpStr.data()), grpStr.size());

    reqLen = addOffsMsg.encode(reqBuf_.data());
    ASSERT_GT(reqLen, 0u);
    outLen = svc_->handleAddOffsets(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    ASSERT_EQ(outLen, pc::AddOffsetsToTxnResponse::ENCODED_SIZE);
    auto addOffsResp = pc::AddOffsetsToTxnResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(addOffsResp);
    EXPECT_EQ(addOffsResp->errorCode, 0) << "AddOffsetsToTxn should succeed while Ongoing";
}

// Task 11 fix 2 (B1): see GroupCoordinationServiceRegression twin.
TEST(TransactionCoordinationServiceRegression, PushedLeadershipNeverCallsTheLeaderFunction) {
    int calls = 0;
    rune::node::TransactionCoordinationService svc(nullptr, nullptr, [&calls] { ++calls; return false; });
    svc.setLocalLeadership(false);
    for (int i = 0; i < 100; ++i) (void)svc.doWork(i);
    EXPECT_FALSE(svc.isLocalCoordinator());
    EXPECT_EQ(calls, 0) << "isLeaderFn_ called on the duty cycle after a leadership push";
}

// ===========================================================================
// No-hang wave (R2): every txn handler answers a request whose body does not
// decode with its own response type + InvalidRequest (it used to return 0 —
// indistinguishable from EndTxn's deferred answer, so the gateway dropped it).
// ===========================================================================
TEST_F(TransactionCoordinationServiceTest, MalformedTxnRequestsAreAnsweredInvalidRequestRegression) {
    constexpr auto kInv = static_cast<int16_t>(pc::ErrorCode::InvalidRequest);
    auto headerOnly = [&](pc::TemplateId tid, int64_t corr) {
        pc::MessageHeader h;
        h.blockLength = 32; h.templateId = static_cast<uint16_t>(tid);
        h.sessionId = 42; h.correlationId = corr;
        h.encode(reqBuf_.data());
        return pc::MessageHeader::ENCODED_SIZE;
    };
    std::size_t o = svc_->handleInitProducerId(reqBuf_.data(), headerOnly(pc::TemplateId::InitProducerId, 1),
                                               0, outBuf_.data(), outBuf_.size());
    auto ip = pc::InitProducerIdResponse::decode(outBuf_.data(), o);
    ASSERT_TRUE(ip.has_value()); EXPECT_EQ(ip->correlationId, 1); EXPECT_EQ(ip->errorCode, kInv);

    o = svc_->handleAddPartitions(reqBuf_.data(), headerOnly(pc::TemplateId::AddPartitionsToTxn, 2),
                                  0, outBuf_.data(), outBuf_.size());
    auto ap = pc::AddPartitionsToTxnResponse::decode(outBuf_.data(), o);
    ASSERT_TRUE(ap.has_value()); EXPECT_EQ(ap->correlationId, 2);
    ASSERT_EQ(ap->results.size(), 1u); EXPECT_EQ(ap->results[0].errorCode, kInv);

    o = svc_->handleAddOffsets(reqBuf_.data(), headerOnly(pc::TemplateId::AddOffsetsToTxn, 3),
                               0, outBuf_.data(), outBuf_.size());
    auto ao = pc::AddOffsetsToTxnResponse::decode(outBuf_.data(), o);
    ASSERT_TRUE(ao.has_value()); EXPECT_EQ(ao->correlationId, 3); EXPECT_EQ(ao->errorCode, kInv);

    o = svc_->handleTxnOffsetCommit(reqBuf_.data(), headerOnly(pc::TemplateId::TxnOffsetCommit, 4),
                                    0, outBuf_.data(), outBuf_.size());
    auto tc = pc::TxnOffsetCommitResponse::decode(outBuf_.data(), o);
    ASSERT_TRUE(tc.has_value()); EXPECT_EQ(tc->correlationId, 4);
    ASSERT_EQ(tc->results.size(), 1u); EXPECT_EQ(tc->results[0].errorCode, kInv);

    o = svc_->handleEndTxn(reqBuf_.data(), headerOnly(pc::TemplateId::EndTxn, 5),
                           0, outBuf_.data(), outBuf_.size());
    auto et = pc::EndTxnResponse::decode(outBuf_.data(), o);
    ASSERT_TRUE(et.has_value()); EXPECT_EQ(et->correlationId, 5); EXPECT_EQ(et->errorCode, kInv);
}

// No-hang fix round 2 (I3): a response that does not fit the caller's buffer is
// answered MessageTooLarge (its own response type) — it used to return 0, which
// the gateway reads as "deferred" and never answers.
TEST_F(TransactionCoordinationServiceTest, ResponseThatCannotFitIsAnsweredMessageTooLargeRegression) {
    std::size_t reqLen = encodeInitProducerId(reqBuf_.data(), reqBuf_.size(), 1, 1, 30'000, "txnBig");
    std::size_t outLen = svc_->handleInitProducerId(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    const auto init = pc::InitProducerIdResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(init);
    std::vector<pc::TxnTopicPartition> parts;
    for (int32_t p = 0; p < 20; ++p) parts.push_back({0, p});   // 24 + 2 + 20*10 B > 64 B
    reqLen = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), 1, 9, init->producerId, init->producerEpoch,
                                 "txnBig", parts);
    ASSERT_GT(reqLen, 0u);
    constexpr std::size_t kSmallCap = 64;   // kMaxRejectionBytes: room for the rejection only
    outLen = svc_->handleAddPartitions(reqBuf_.data(), reqLen, 0, outBuf_.data(), kSmallCap);
    ASSERT_GT(outLen, 0u) << "a response that cannot fit returned 0 (= 'deferred', never answered)";
    const auto r = pc::AddPartitionsToTxnResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(r);
    EXPECT_EQ(r->correlationId, 9);
    ASSERT_EQ(r->results.size(), 1u);
    EXPECT_EQ(r->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));
}

// Fix round 3 (I3): handleEndTxn never returns the "deferred" 0 for an answer it
// could not write.  At the contract minimum (kTxnMinOutCap) every EndTxn outcome
// is written; below it the handler halts loudly instead of returning 0.
TEST_F(TransactionCoordinationServiceTest, EndTxnAtMinOutCapIsAlwaysAnsweredRegression) {
    std::array<uint8_t, rune::gateway::kTxnMinOutCap> small{};
    // A malformed EndTxn (rejection) and a not-coordinator EndTxn, both at the minimum.
    pc::MessageHeader h;
    h.blockLength = 32; h.templateId = static_cast<uint16_t>(pc::TemplateId::EndTxn);
    h.sessionId = 42; h.correlationId = 7;
    h.encode(reqBuf_.data());
    std::size_t n = svc_->handleEndTxn(reqBuf_.data(), pc::MessageHeader::ENCODED_SIZE, 0, small.data(), small.size());
    ASSERT_GT(n, 0u) << "a malformed EndTxn yielded the 'deferred' 0";
    EXPECT_EQ(pc::EndTxnResponse::decode(small.data(), n)->correlationId, 7);
    isLeader_ = false;
    const std::size_t reqLen = encodeEndTxn(reqBuf_.data(), reqBuf_.size(), 42, 8, 10, 0, true, "txA");
    n = svc_->handleEndTxn(reqBuf_.data(), reqLen, 0, small.data(), small.size());
    ASSERT_GT(n, 0u);
    EXPECT_EQ(pc::EndTxnResponse::decode(small.data(), n)->errorCode,
              static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
}

TEST_F(TransactionCoordinationServiceTest, EndTxnBelowMinOutCapHaltsInsteadOfDeferringRegression) {
    isLeader_ = false;
    const std::size_t reqLen = encodeEndTxn(reqBuf_.data(), reqBuf_.size(), 42, 9, 10, 0, true, "txA");
    std::array<uint8_t, 8> tiny{};
    EXPECT_DEATH((void)svc_->handleEndTxn(reqBuf_.data(), reqLen, 0, tiny.data(), tiny.size()),
                 "kTxnMinOutCap");
}

// ===========================================================================
// Phase 2b F Task 11 (fix round 1): a refused transactional-offset apply defers the commit, never loses the offset.
// ===========================================================================
namespace {
struct RefusingSink {
    int refusals = 0;          ///< refuse this many applies, then accept
    int calls    = 0;
    int accepted = 0;
    std::vector<int64_t> acceptedOffsets;
    int replicating = 0;       ///< #358: then answer Replicating this many times before Applied
    std::vector<int64_t> offered;
    static rune::coordination::TxnOffsetApply apply(void* ctx, const std::string&, const rune::coordination::TopicPartition&,
                                                    int64_t offset, int64_t, const rune::coordination::TxnOffsetToken&) {
        auto* s = static_cast<RefusingSink*>(ctx);
        ++s->calls;
        s->offered.push_back(offset);
        if (s->refusals > 0) { --s->refusals; return rune::coordination::TxnOffsetApply::Refused; }
        if (s->replicating > 0) { --s->replicating; return rune::coordination::TxnOffsetApply::Replicating; }
        ++s->accepted;
        s->acceptedOffsets.push_back(offset);
        return rune::coordination::TxnOffsetApply::Applied;
    }
    int released = 0;
    std::vector<rune::coordination::TxnOffsetToken> tokens;
    static void release(void* ctx, const rune::coordination::TxnOffsetToken& t) {
        auto* s = static_cast<RefusingSink*>(ctx);
        ++s->released;
        s->tokens.push_back(t);
    }
};
}  // namespace

TEST_F(TransactionCoordinationServiceTest, RefusedOffsetApplyKeepsPrepareCommitAndRetriesUntilAcceptedRegression) {
    RefusingSink sink;
    sink.refusals = 3;

    const std::string txn = "txnOffsetRetry";
    std::size_t reqLen = encodeInitProducerId(reqBuf_.data(), reqBuf_.size(), 1, 1, 30'000, txn);
    std::size_t outLen = svc_->handleInitProducerId(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    auto init = pc::InitProducerIdResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(init);
    const int64_t pid = init->producerId;
    const int16_t ep  = init->producerEpoch;

    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    reqLen = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), 1, 2, pid, ep, txn, parts);
    ASSERT_GT(svc_->handleAddPartitions(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size()), 0u);

    const std::string grp = "cg-retry";
    pc::AddOffsetsToTxn add;
    add.sessionId = 1; add.correlationId = 3; add.producerId = pid; add.producerEpoch = ep;
    add.transactionalId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(txn.data()), txn.size());
    add.groupId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(grp.data()), grp.size());
    reqLen = add.encode(reqBuf_.data());
    ASSERT_GT(svc_->handleAddOffsets(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size()), 0u);

    pc::TxnOffsetCommit toc;
    toc.sessionId = 1; toc.correlationId = 4; toc.producerId = pid; toc.producerEpoch = ep;
    toc.transactionalId = add.transactionalId;
    toc.groupId = add.groupId;
    const std::vector<pc::TxnOffsetCommitEntry> entries = {{0, 0, 42, {}}};
    reqLen = toc.encode(reqBuf_.data(), entries);
    ASSERT_GT(svc_->handleTxnOffsetCommit(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size()), 0u);

    reqLen = encodeEndTxn(reqBuf_.data(), reqBuf_.size(), 1, 5, pid, ep, /*commit=*/true, txn);
    outLen = svc_->handleEndTxn(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    ASSERT_EQ(pc::EndTxnResponse::decode(outBuf_.data(), outLen)->errorCode, 0);
    ASSERT_EQ(svc_->txnStateForTest(txn), tx::TxnState::PrepareCommit);

    // The controller fails over: a NEW coordinator loads the durable PrepareCommit (with its pending offsets) and
    // re-drives its markers - exactly where the group coordinator's offsets may not be loaded yet.
    svc_ = std::make_unique<rune::node::TransactionCoordinationService>(
        txnStore_.get(), /*topics=*/nullptr, [this] { return isLeader_; }, [this] { return leaderNodeId_; });
    svc_->setTxnOffsetSink(&RefusingSink::apply, &sink);
    svc_->setAppendFnOverride([](std::string_view, int32_t, std::span<const uint8_t>) -> int64_t { return 0; });
    int64_t now = 1'000'000'000;
    (void)svc_->doWork(now);                                   // loads, dispatches the markers (the seam: outstanding)
    ASSERT_EQ(svc_->txnStateForTest(txn), tx::TxnState::PrepareCommit);
    ASSERT_EQ(svc_->awaitingMarkerTxnsForTest(), 1u);
    svc_->onControlMarkerLanded(0, 0, pid, rune::gateway::ITxnRequestHandler::kAnyCoordinatorEpoch);   // last marker landed

    // The sink refuses 3 times: PrepareCommit holds, the commit is not completed, the offset is not applied.
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(svc_->txnStateForTest(txn), tx::TxnState::PrepareCommit) << "refusal " << i;
        EXPECT_EQ(svc_->transactionsCompletedForTest(), 0u) << "EndTxn is not answered while the apply is refused";
        EXPECT_EQ(svc_->awaitingMarkerTxnsForTest(), 1u) << "the deferred EndTxn is still awaiting";
        EXPECT_EQ(sink.accepted, 0);
        now += 1'100'000'000;   // beyond the 1 s backoff cap
        (void)svc_->doWork(now);
    }
    EXPECT_EQ(sink.calls, 4) << "3 refusals + the accepted retry";
    EXPECT_EQ(sink.accepted, 1) << "the offset is applied exactly once";
    EXPECT_EQ(sink.acceptedOffsets, (std::vector<int64_t>{42}));
    EXPECT_EQ(svc_->txnStateForTest(txn), tx::TxnState::CompleteCommit);
    EXPECT_EQ(svc_->transactionsCompletedForTest(), 1u);
    EXPECT_EQ(svc_->awaitingMarkerTxnsForTest(), 0u);
    EXPECT_EQ(svc_->txnOffsetApplyRetries(), 3u);

    // Nothing re-applies a completed commit.
    now += 1'100'000'000;
    (void)svc_->doWork(now);
    EXPECT_EQ(sink.accepted, 1);
}

// ===========================================================================
// #358: CompleteCommit waits until every committed offset record is COMMITTED on the offsets replica.  While the
// sink answers Replicating the transaction stays PrepareCommit (nothing persisted, EndTxn not answered) and the
// completion is re-checked on the very next doWork (a replication wait, not a refusal: no backoff); every offset is
// offered on every pass, so they replicate together.
// ===========================================================================
TEST_F(TransactionCoordinationServiceTest, CompleteCommitWaitsUntilTheOffsetRecordsAreCommittedRegression) {
    RefusingSink sink;
    sink.replicating = 4;   // the two offsets answer Replicating on two passes, then Applied
    svc_->setTxnOffsetSink(&RefusingSink::apply, &sink);
    svc_->setAppendFnOverride([](std::string_view, int32_t, std::span<const uint8_t>) -> int64_t { return 0; });

    const std::string txn = "txnOffsetHw";
    std::size_t reqLen = encodeInitProducerId(reqBuf_.data(), reqBuf_.size(), 1, 1, 30'000, txn);
    std::size_t outLen = svc_->handleInitProducerId(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    auto init = pc::InitProducerIdResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(init);
    const int64_t pid = init->producerId;
    const int16_t ep  = init->producerEpoch;
    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    reqLen = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), 1, 2, pid, ep, txn, parts);
    ASSERT_GT(svc_->handleAddPartitions(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size()), 0u);
    const std::string grp = "cg-hw";
    pc::AddOffsetsToTxn add;
    add.sessionId = 1; add.correlationId = 3; add.producerId = pid; add.producerEpoch = ep;
    add.transactionalId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(txn.data()), txn.size());
    add.groupId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(grp.data()), grp.size());
    reqLen = add.encode(reqBuf_.data());
    ASSERT_GT(svc_->handleAddOffsets(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size()), 0u);
    pc::TxnOffsetCommit toc;
    toc.sessionId = 1; toc.correlationId = 4; toc.producerId = pid; toc.producerEpoch = ep;
    toc.transactionalId = add.transactionalId;
    toc.groupId = add.groupId;
    const std::vector<pc::TxnOffsetCommitEntry> entries = {{0, 0, 42, {}}, {0, 1, 43, {}}};
    reqLen = toc.encode(reqBuf_.data(), entries);
    ASSERT_GT(svc_->handleTxnOffsetCommit(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size()), 0u);

    reqLen = encodeEndTxn(reqBuf_.data(), reqBuf_.size(), 1, 5, pid, ep, /*commit=*/true, txn);
    (void)svc_->handleEndTxn(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    ASSERT_EQ(svc_->txnStateForTest(txn), tx::TxnState::PrepareCommit);
    // The rig has no gateway: a coordinator that loads the durable PrepareCommit re-drives its markers through the
    // append seam (outstanding until they land), exactly the re-drive path of a failover.
    svc_ = std::make_unique<rune::node::TransactionCoordinationService>(
        txnStore_.get(), /*topics=*/nullptr, [this] { return isLeader_; }, [this] { return leaderNodeId_; });
    svc_->setTxnOffsetSink(&RefusingSink::apply, &sink, &RefusingSink::release);
    svc_->setAppendFnOverride([](std::string_view, int32_t, std::span<const uint8_t>) -> int64_t { return 0; });
    int64_t now = 1'000'000'000;
    (void)svc_->doWork(now);
    ASSERT_EQ(svc_->awaitingMarkerTxnsForTest(), 1u);
    svc_->onControlMarkerLanded(0, 0, pid, rune::gateway::ITxnRequestHandler::kAnyCoordinatorEpoch);

    // Pass 1 (the landing marker) and pass 2 (the next doWork, 1 us later): both offsets offered, Replicating.
    EXPECT_EQ(sink.offered, (std::vector<int64_t>{42, 43})) << "every offset is offered on the first pass";
    EXPECT_EQ(svc_->txnStateForTest(txn), tx::TxnState::PrepareCommit) << "never CompleteCommit before the offsets commit";
    EXPECT_EQ(svc_->transactionsCompletedForTest(), 0u);
    EXPECT_EQ(svc_->awaitingMarkerTxnsForTest(), 1u);
    now += 1'000;
    (void)svc_->doWork(now);
    EXPECT_EQ(sink.offered.size(), 4u) << "re-checked on the very next doWork, no backoff";
    EXPECT_EQ(svc_->txnStateForTest(txn), tx::TxnState::PrepareCommit);
    EXPECT_EQ(svc_->transactionsCompletedForTest(), 0u);
    // Pass 3: committed.
    now += 1'000;
    (void)svc_->doWork(now);
    EXPECT_EQ(sink.acceptedOffsets, (std::vector<int64_t>{42, 43}));
    EXPECT_EQ(svc_->txnStateForTest(txn), tx::TxnState::CompleteCommit);
    EXPECT_EQ(svc_->transactionsCompletedForTest(), 1u);
    EXPECT_EQ(svc_->awaitingMarkerTxnsForTest(), 0u);
    EXPECT_EQ(svc_->txnOffsetReplicationWaits(), 2u);
    EXPECT_EQ(svc_->txnOffsetApplyRetries(), 0u) << "a replication wait is not a refusal";
    ASSERT_EQ(sink.released, 1) << "PR #434 re-check: the completed transaction's offset waiters are released";
    EXPECT_EQ(sink.tokens[0].producerId, pid);
    EXPECT_EQ(sink.tokens[0].producerEpoch, ep);
    EXPECT_GE(sink.tokens[0].coordinatorEpoch, 0);
    now += 1'100'000'000;
    (void)svc_->doWork(now);
    EXPECT_EQ(sink.calls, 6) << "nothing re-applies a completed commit";
}

// #358: a refusal met on a pass after replication waits backs off like any refusal; the commit still completes only
// once every offset is Applied.
TEST_F(TransactionCoordinationServiceTest, ARefusalAfterReplicationWaitsBacksOffAndStillWaitsForCommitRegression) {
    RefusingSink sink;
    sink.replicating = 1;
    svc_->setTxnOffsetSink(&RefusingSink::apply, &sink);
    svc_->setAppendFnOverride([](std::string_view, int32_t, std::span<const uint8_t>) -> int64_t { return 0; });
    const std::string txn = "txnOffsetHw2";
    std::size_t reqLen = encodeInitProducerId(reqBuf_.data(), reqBuf_.size(), 1, 1, 30'000, txn);
    std::size_t outLen = svc_->handleInitProducerId(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    auto init = pc::InitProducerIdResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(init);
    const int64_t pid = init->producerId;
    const int16_t ep  = init->producerEpoch;
    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    reqLen = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), 1, 2, pid, ep, txn, parts);
    ASSERT_GT(svc_->handleAddPartitions(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size()), 0u);
    const std::string grp = "cg-hw2";
    pc::AddOffsetsToTxn add;
    add.sessionId = 1; add.correlationId = 3; add.producerId = pid; add.producerEpoch = ep;
    add.transactionalId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(txn.data()), txn.size());
    add.groupId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(grp.data()), grp.size());
    reqLen = add.encode(reqBuf_.data());
    ASSERT_GT(svc_->handleAddOffsets(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size()), 0u);
    pc::TxnOffsetCommit toc;
    toc.sessionId = 1; toc.correlationId = 4; toc.producerId = pid; toc.producerEpoch = ep;
    toc.transactionalId = add.transactionalId;
    toc.groupId = add.groupId;
    const std::vector<pc::TxnOffsetCommitEntry> entries = {{0, 0, 7, {}}};
    reqLen = toc.encode(reqBuf_.data(), entries);
    ASSERT_GT(svc_->handleTxnOffsetCommit(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size()), 0u);
    reqLen = encodeEndTxn(reqBuf_.data(), reqBuf_.size(), 1, 5, pid, ep, /*commit=*/true, txn);
    (void)svc_->handleEndTxn(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    // The rig has no gateway: a coordinator that loads the durable PrepareCommit re-drives its markers through the
    // append seam (outstanding until they land), exactly the re-drive path of a failover.
    svc_ = std::make_unique<rune::node::TransactionCoordinationService>(
        txnStore_.get(), /*topics=*/nullptr, [this] { return isLeader_; }, [this] { return leaderNodeId_; });
    svc_->setTxnOffsetSink(&RefusingSink::apply, &sink);
    svc_->setAppendFnOverride([](std::string_view, int32_t, std::span<const uint8_t>) -> int64_t { return 0; });
    int64_t now = 1'000'000'000;
    (void)svc_->doWork(now);
    ASSERT_EQ(svc_->awaitingMarkerTxnsForTest(), 1u);
    svc_->onControlMarkerLanded(0, 0, pid, rune::gateway::ITxnRequestHandler::kAnyCoordinatorEpoch);
    ASSERT_EQ(sink.calls, 1);                                  // Replicating
    sink.refusals = 1;                                         // the offsets leader moves away: Refused next
    now += 1'000;
    (void)svc_->doWork(now);
    ASSERT_EQ(sink.calls, 2);
    EXPECT_EQ(svc_->txnOffsetApplyRetries(), 1u);
    now += 1'000;
    (void)svc_->doWork(now);
    EXPECT_EQ(sink.calls, 2) << "a refusal backs off (10 ms), it is not re-asked on the next microsecond";
    EXPECT_EQ(svc_->txnStateForTest(txn), tx::TxnState::PrepareCommit);
    now += 20'000'000;
    (void)svc_->doWork(now);
    EXPECT_EQ(sink.calls, 3);
    EXPECT_EQ(svc_->txnStateForTest(txn), tx::TxnState::CompleteCommit);
}

// PR #434 re-check: a coordinator that unloads while a commit waits on its offsets releases those waiters.
TEST_F(TransactionCoordinationServiceTest, AnUnloadReleasesTheOffsetWaitersOfItsWaitingCommitsRegression) {
    RefusingSink sink;
    sink.replicating = 100;
    const std::string txn = "txnOffsetRel";
    std::size_t reqLen = encodeInitProducerId(reqBuf_.data(), reqBuf_.size(), 1, 1, 30'000, txn);
    std::size_t outLen = svc_->handleInitProducerId(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    auto init = pc::InitProducerIdResponse::decode(outBuf_.data(), outLen);
    ASSERT_TRUE(init);
    const int64_t pid = init->producerId;
    const int16_t ep  = init->producerEpoch;
    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    reqLen = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), 1, 2, pid, ep, txn, parts);
    ASSERT_GT(svc_->handleAddPartitions(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size()), 0u);
    const std::string grp = "cg-rel";
    pc::AddOffsetsToTxn add;
    add.sessionId = 1; add.correlationId = 3; add.producerId = pid; add.producerEpoch = ep;
    add.transactionalId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(txn.data()), txn.size());
    add.groupId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(grp.data()), grp.size());
    reqLen = add.encode(reqBuf_.data());
    ASSERT_GT(svc_->handleAddOffsets(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size()), 0u);
    pc::TxnOffsetCommit toc;
    toc.sessionId = 1; toc.correlationId = 4; toc.producerId = pid; toc.producerEpoch = ep;
    toc.transactionalId = add.transactionalId;
    toc.groupId = add.groupId;
    const std::vector<pc::TxnOffsetCommitEntry> entries = {{0, 0, 9, {}}};
    reqLen = toc.encode(reqBuf_.data(), entries);
    ASSERT_GT(svc_->handleTxnOffsetCommit(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size()), 0u);
    reqLen = encodeEndTxn(reqBuf_.data(), reqBuf_.size(), 1, 5, pid, ep, /*commit=*/true, txn);
    (void)svc_->handleEndTxn(reqBuf_.data(), reqLen, 0, outBuf_.data(), outBuf_.size());
    svc_ = std::make_unique<rune::node::TransactionCoordinationService>(
        txnStore_.get(), /*topics=*/nullptr, [this] { return isLeader_; }, [this] { return leaderNodeId_; });
    svc_->setTxnOffsetSink(&RefusingSink::apply, &sink, &RefusingSink::release);
    svc_->setAppendFnOverride([](std::string_view, int32_t, std::span<const uint8_t>) -> int64_t { return 0; });
    int64_t now = 1'000'000'000;
    (void)svc_->doWork(now);
    svc_->onControlMarkerLanded(0, 0, pid, rune::gateway::ITxnRequestHandler::kAnyCoordinatorEpoch);
    ASSERT_GE(sink.calls, 1);
    EXPECT_EQ(sink.released, 0) << "still waiting";
    isLeader_ = false;
    (void)svc_->doWork(now + 1'000);
    ASSERT_EQ(sink.released, 1) << "the unloaded coordinator lets its offset waiters go";
    EXPECT_EQ(sink.tokens[0].producerId, pid);
}

// ===========================================================================
// Issue #122: InitProducerId issues the id bound to the session's principal; AddPartitions / EndTxn naming another
// principal's id are answered exactly like any wrong producer id and change nothing.  Sessions 1 and 3 are user 7,
// session 2 is user 8.
// ===========================================================================
namespace {
uint64_t txnPrincipalBySession(void*, int64_t sid) noexcept { return sid == 2 ? 8 : 7; }
}  // namespace

TEST_F(TransactionCoordinationServiceTest, AnotherPrincipalsProducerIdIsAnsweredAsAWrongIdRegression) {
    svc_->setPrincipalResolverForTest(&txnPrincipalBySession, nullptr);
    std::size_t n = svc_->handleInitProducerId(reqBuf_.data(), encodeInitProducerId(reqBuf_.data(), reqBuf_.size(), 1, 1,
                                                                                    30'000, "txA"),
                                               0, outBuf_.data(), outBuf_.size());
    const auto init = pc::InitProducerIdResponse::decode(outBuf_.data(), n);
    ASSERT_TRUE(init);
    ASSERT_EQ(init->errorCode, 0);
    EXPECT_TRUE(rune::protocol::producerIdOwnedBy(init->producerId, 7));
    const int64_t pid = init->producerId;
    const int16_t epoch = init->producerEpoch;
    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    auto addCode = [&](int64_t sid, int64_t p) {
        const std::size_t len = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), sid, 2, p, epoch, "txA", parts);
        const std::size_t out = svc_->handleAddPartitions(reqBuf_.data(), len, 0, outBuf_.data(), outBuf_.size());
        const auto r = pc::AddPartitionsToTxnResponse::decode(outBuf_.data(), out);
        return r && !r->results.empty() ? static_cast<int>(r->results[0].errorCode) : -1;
    };
    auto endCode = [&](int64_t sid, int64_t p, bool commit) {
        const std::size_t len = encodeEndTxn(reqBuf_.data(), reqBuf_.size(), sid, 3, p, epoch, commit, "txA");
        const std::size_t out = svc_->handleEndTxn(reqBuf_.data(), len, 0, outBuf_.data(), outBuf_.size());
        const auto r = pc::EndTxnResponse::decode(outBuf_.data(), out);
        return r ? static_cast<int>(r->errorCode) : -1;
    };
    const int wrongId = addCode(2, 99);
    EXPECT_NE(wrongId, 0);
    EXPECT_EQ(addCode(2, pid), wrongId) << "another principal's id must look like any wrong id";
    ASSERT_EQ(addCode(3, pid), 0) << "the owner (on another session) adds the partition";
    EXPECT_EQ(endCode(2, pid, /*commit=*/false), endCode(2, 99, false)) << "a foreign EndTxn is a wrong-id EndTxn";
    EXPECT_EQ(svc_->txnStateForTest("txA"), tx::TxnState::Ongoing) << "the foreign EndTxn aborted the owner's transaction";

    // Principal 8 taking the transactional id over gets a fresh id of its own.
    n = svc_->handleInitProducerId(reqBuf_.data(), encodeInitProducerId(reqBuf_.data(), reqBuf_.size(), 2, 4, 30'000, "txA"),
                                   0, outBuf_.data(), outBuf_.size());
    const auto taken = pc::InitProducerIdResponse::decode(outBuf_.data(), n);
    ASSERT_TRUE(taken);
    ASSERT_EQ(taken->errorCode, 0);
    EXPECT_NE(taken->producerId, pid);
    EXPECT_TRUE(rune::protocol::producerIdOwnedBy(taken->producerId, 8));
}

// PR #423 review F1 (issue #122, rolling upgrade): bound producer ids are issued only once the committed metadata
// feature level says every member supports them.  Below it, ids are raw and a transactional id is never rebound
// (an older coordinator may take over and must never see a bound id); at the level, ids are bound and a raw id held
// by a transactional id is rebound to its caller.
TEST_F(TransactionCoordinationServiceTest, BoundProducerIdsWaitForTheClusterFeatureLevelRegression) {
    namespace rr = rune::replication;
    svc_->setPrincipalResolverForTest(&txnPrincipalBySession, nullptr);
    auto init = [&](int64_t sid, std::string_view txnId) {
        const std::size_t n = svc_->handleInitProducerId(
            reqBuf_.data(), encodeInitProducerId(reqBuf_.data(), reqBuf_.size(), sid, 1, 30'000, txnId), 0,
            outBuf_.data(), outBuf_.size());
        const auto r = pc::InitProducerIdResponse::decode(outBuf_.data(), n);
        EXPECT_TRUE(r && r->errorCode == 0);
        return r ? std::pair<int64_t, int16_t>{r->producerId, r->producerEpoch} : std::pair<int64_t, int16_t>{-1, -1};
    };
    svc_->setMetadataFeatureLevel(rr::MetadataFeature::kStorageIdentity);   // a member still runs an older binary
    EXPECT_FALSE(svc_->bindsProducerIds());
    const auto idem = init(1, "");
    EXPECT_GE(idem.first, 0);
    EXPECT_FALSE(rune::protocol::isBoundProducerId(idem.first)) << "a bound id was issued in a mixed cluster";
    const auto a = init(1, "tx-gate");
    const auto b = init(2, "tx-gate");
    EXPECT_FALSE(rune::protocol::isBoundProducerId(a.first));
    EXPECT_EQ(b.first, a.first) << "below the level a transactional id keeps its id (no rebinding)";
    EXPECT_EQ(b.second, a.second + 1);

    svc_->setMetadataFeatureLevel(rr::MetadataFeature::kBoundProducerIds);   // every member upgraded
    EXPECT_TRUE(svc_->bindsProducerIds());
    const auto bound = init(1, "");
    EXPECT_TRUE(rune::protocol::producerIdOwnedBy(bound.first, 7));
    EXPECT_GT(rune::protocol::producerIdCounter(bound.first), idem.first) << "the counter continues past the raw ids";
    const auto rebound = init(2, "tx-gate");
    EXPECT_NE(rebound.first, a.first) << "the raw id, owned by nobody, is rebound at the level";
    EXPECT_TRUE(rune::protocol::producerIdOwnedBy(rebound.first, 8));
}

// ===========================================================================
// #358 class: nothing that depends on a __transaction_state record happens before the record is COMMITTED.  The
// txn-state leader (node 1) has follower 2 in its ISR, so an acks-all append commits only once 2 fetched it.  An
// answer that follows an append is parked until then (an AddPartitions acked before its registration commits is
// lost by a coordinator failover: the new coordinator never writes that partition's marker and the partition's
// LSO stays pinned - the failover hang of #112); a transaction's markers wait for its Prepare* record.
// ===========================================================================
namespace {
struct CapturedAnswers {
    std::vector<std::vector<uint8_t>> frames;
    static void sink(void* ctx, int64_t, const uint8_t* f, std::size_t n) {
        static_cast<CapturedAnswers*>(ctx)->frames.emplace_back(f, f + n);
    }
};
}  // namespace

class TxnStateReplicationTest : public ::testing::Test {
protected:
    TxnStateReplicationTest() : dir_("svc_repl"), log_(dir_.path) {}
    void SetUp() override {
        transport_.reset(new NullTransport{});
        replica_ = std::make_unique<rep::PartitionReplica>(makeSingleNodeConfig(), rep::Role::Leader, /*epoch=*/1,
                                                           std::vector<rep::NodeId>{1, 2}, std::vector<rep::NodeId>{1, 2},
                                                           &log_, transport_.get());
        txnStore_ = std::make_unique<tx::ReplicatedTxnStateStore>(replica_.get());
        svc_ = std::make_unique<rune::node::TransactionCoordinationService>(
            txnStore_.get(), /*topics=*/nullptr, [this] { return isLeader_; }, [] { return rune::node::NodeId{1}; });
        svc_->setAnswerSinkForTest(&CapturedAnswers::sink, &answers_);
        (void)svc_->doWork(now_);                  // loads; requests the first pid block ceiling
        followerFetches();
        (void)svc_->doWork(now_);                  // the ceiling is committed: pids can be handed out
    }
    /// Node 2 fetches up to the leader's log end: everything appended so far commits.
    void followerFetches() {
        rep::FetchRequestMsg req{};
        req.leaderEpoch = replica_->epoch(); req.partition = 0; req.followerId = 2;
        req.fetchOffset = replica_->logEndOffset(); req.maxBytes = 1 << 20; req.topicId = 0;
        now_ += 1'000'000;
        (void)replica_->onFetchRequest(req, now_);
        replica_->doWork(now_);
    }
    int64_t initPid(const std::string& txn, int16_t& epoch, int32_t timeoutMs = 30'000) {
        const std::size_t reqLen = encodeInitProducerId(reqBuf_.data(), reqBuf_.size(), 1, ++corr_, timeoutMs, txn);
        EXPECT_EQ(svc_->handleInitProducerId(reqBuf_.data(), reqLen, now_, outBuf_.data(), outBuf_.size()), 0u)
            << "the epoch is answered only once its record commits";
        EXPECT_EQ(svc_->parkedAnswers(), 1u);
        EXPECT_TRUE(answers_.frames.empty());
        followerFetches();
        (void)svc_->doWork(now_);
        EXPECT_EQ(svc_->parkedAnswers(), 0u);
        EXPECT_EQ(answers_.frames.size(), 1u);
        const auto r = pc::InitProducerIdResponse::decode(answers_.frames.back().data(), answers_.frames.back().size());
        EXPECT_TRUE(r && r->errorCode == 0);
        answers_.frames.clear();
        epoch = r ? r->producerEpoch : 0;
        return r ? r->producerId : -1;
    }

    TmpDir dir_;
    rune::storage::Log log_;
    std::unique_ptr<rep::PartitionTransport>          transport_;
    std::unique_ptr<rep::PartitionReplica>            replica_;
    std::unique_ptr<tx::ReplicatedTxnStateStore>      txnStore_;
    std::unique_ptr<rune::node::TransactionCoordinationService> svc_;
    CapturedAnswers answers_;
    bool    isLeader_ = true;
    int64_t now_      = 1'000'000'000;
    int64_t corr_     = 0;
    std::array<uint8_t, 1024> reqBuf_{};
    std::array<uint8_t, 1024> outBuf_{};
};

TEST_F(TxnStateReplicationTest, AnAddPartitionsIsAnsweredOnlyOnceItsRegistrationCommitsRegression) {
    int16_t ep = 0;
    const int64_t pid = initPid("txReg", ep);
    ASSERT_GE(pid, 0);
    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}, {0, 1}};
    const std::size_t reqLen = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), 1, ++corr_, pid, ep, "txReg", parts);
    EXPECT_EQ(svc_->handleAddPartitions(reqBuf_.data(), reqLen, now_, outBuf_.data(), outBuf_.size()), 0u);
    EXPECT_EQ(svc_->parkedAnswers(), 1u);
    (void)svc_->doWork(now_ + 1);
    EXPECT_TRUE(answers_.frames.empty()) << "no OK while the registration is only on the leader";
    followerFetches();
    (void)svc_->doWork(now_);
    ASSERT_EQ(answers_.frames.size(), 1u);
    const auto r = pc::AddPartitionsToTxnResponse::decode(answers_.frames[0].data(), answers_.frames[0].size());
    ASSERT_TRUE(r);
    ASSERT_EQ(r->results.size(), 2u);
    EXPECT_EQ(r->results[0].errorCode, 0);
    EXPECT_EQ(r->results[1].errorCode, 0);
}

TEST_F(TxnStateReplicationTest, AParkedAnswerIsNotCoordinatorWhenTheCoordinatorUnloadsFirstRegression) {
    int16_t ep = 0;
    const int64_t pid = initPid("txLost", ep);
    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    const std::size_t reqLen = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), 1, ++corr_, pid, ep, "txLost", parts);
    ASSERT_EQ(svc_->handleAddPartitions(reqBuf_.data(), reqLen, now_, outBuf_.data(), outBuf_.size()), 0u);
    isLeader_ = false;                          // the coordinator moves before the registration committed
    (void)svc_->doWork(now_ + 1);
    ASSERT_EQ(answers_.frames.size(), 1u);
    const auto r = pc::AddPartitionsToTxnResponse::decode(answers_.frames[0].data(), answers_.frames[0].size());
    ASSERT_TRUE(r);
    ASSERT_GE(r->results.size(), 1u);
    EXPECT_EQ(r->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::NotCoordinator))
        << "never an OK for a registration the next coordinator may not have: the client retries there";
    EXPECT_EQ(svc_->parkedAnswers(), 0u);
}

TEST_F(TxnStateReplicationTest, AbortMarkersWaitForThePrepareAbortRecordToCommitRegression) {
    int markerAppends = 0;
    svc_->setAppendFnOverride([&](std::string_view, int32_t, std::span<const uint8_t>) -> int64_t {
        ++markerAppends;
        return 0;
    });
    int16_t ep = 0;
    const int64_t pid = initPid("txTimeout", ep, /*timeoutMs=*/100);
    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    const std::size_t reqLen = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), 1, ++corr_, pid, ep, "txTimeout", parts);
    ASSERT_EQ(svc_->handleAddPartitions(reqBuf_.data(), reqLen, now_, outBuf_.data(), outBuf_.size()), 0u);
    followerFetches();
    (void)svc_->doWork(now_);
    ASSERT_EQ(answers_.frames.size(), 1u);
    now_ += 1'000'000'000;                      // past the 100 ms transaction timeout
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnStateForTest("txTimeout"), tx::TxnState::PrepareAbort);
    EXPECT_EQ(markerAppends, 0) << "no marker before the PrepareAbort record commits";
    EXPECT_EQ(svc_->prepareGates(), 1u);
    (void)svc_->doWork(now_ + 1);
    EXPECT_EQ(markerAppends, 0);
    followerFetches();
    (void)svc_->doWork(now_);
    EXPECT_EQ(markerAppends, 1) << "the abort marker goes out once its record is committed";
    EXPECT_EQ(svc_->prepareGates(), 0u);
    EXPECT_EQ(svc_->awaitingMarkerTxnsForTest(), 1u);
}

// ===========================================================================
// Adversarial review of PR #434 (#358 class): the parked-answer / Prepare-gate machinery driven the way a hostile or
// buggy client and a failing cluster would: pipelined and replayed requests from several sessions, a leadership lost
// and regained under a parked answer, an append refused after a demotion the coordinator has not noticed yet, and a
// coordinator restarted over an uncommitted tail.
// ===========================================================================
namespace {
struct SessionAnswers {
    struct A { int64_t session; std::vector<uint8_t> frame; };
    std::vector<A> got;
    static void sink(void* ctx, int64_t session, const uint8_t* f, std::size_t n) {
        static_cast<SessionAnswers*>(ctx)->got.push_back({session, std::vector<uint8_t>(f, f + n)});
    }
    [[nodiscard]] int64_t corrOf(std::size_t i) const {
        const auto h = pc::MessageHeader::decode(got[i].frame.data(), got[i].frame.size());
        return h ? h->correlationId : -1;
    }
};
}  // namespace

TEST_F(TxnStateReplicationTest, PipelinedAnswersOfSeveralSessionsAreReleasedInRecordOrderToTheirOwnSessionAdversarialRegression) {
    SessionAnswers sa;
    svc_->setAnswerSinkForTest(&SessionAnswers::sink, &sa);
    // Two sessions, three requests back to back, none committed yet; a replay of the first one among them.
    std::size_t n = encodeInitProducerId(reqBuf_.data(), reqBuf_.size(), /*session=*/1, /*corr=*/101, 30'000, "txA");
    ASSERT_EQ(svc_->handleInitProducerId(reqBuf_.data(), n, now_, outBuf_.data(), outBuf_.size()), 0u);
    n = encodeInitProducerId(reqBuf_.data(), reqBuf_.size(), /*session=*/3, /*corr=*/301, 30'000, "txB");
    ASSERT_EQ(svc_->handleInitProducerId(reqBuf_.data(), n, now_, outBuf_.data(), outBuf_.size()), 0u);
    n = encodeInitProducerId(reqBuf_.data(), reqBuf_.size(), /*session=*/1, /*corr=*/102, 30'000, "txA");   // replay
    ASSERT_EQ(svc_->handleInitProducerId(reqBuf_.data(), n, now_, outBuf_.data(), outBuf_.size()), 0u);
    EXPECT_EQ(svc_->parkedAnswers(), 3u);
    for (int i = 0; i < 3; ++i) (void)svc_->doWork(now_ + 1 + i);
    EXPECT_TRUE(sa.got.empty()) << "nothing is answered while every record is only on the leader";
    followerFetches();
    (void)svc_->doWork(now_);
    ASSERT_EQ(sa.got.size(), 3u);
    EXPECT_EQ(sa.got[0].session, 1); EXPECT_EQ(sa.corrOf(0), 101);
    EXPECT_EQ(sa.got[1].session, 3); EXPECT_EQ(sa.corrOf(1), 301);
    EXPECT_EQ(sa.got[2].session, 1); EXPECT_EQ(sa.corrOf(2), 102);
    const auto a1 = pc::InitProducerIdResponse::decode(sa.got[0].frame.data(), sa.got[0].frame.size());
    const auto a2 = pc::InitProducerIdResponse::decode(sa.got[2].frame.data(), sa.got[2].frame.size());
    ASSERT_TRUE(a1 && a2);
    EXPECT_EQ(a1->errorCode, 0);
    EXPECT_EQ(a2->errorCode, 0);
    EXPECT_EQ(a2->producerId, a1->producerId);
    EXPECT_GT(a2->producerEpoch, a1->producerEpoch) << "the replay fences the first epoch, as a fresh InitProducerId";
    EXPECT_EQ(svc_->parkedAnswers(), 0u);
}

TEST_F(TxnStateReplicationTest, ALeadershipLostAndRegainedUnderAParkedAnswerAnswersNotCoordinatorAdversarialRegression) {
    int16_t ep = 0;
    const int64_t pid = initPid("txAba", ep);
    ASSERT_GE(pid, 0);
    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    std::size_t n = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), 1, ++corr_, pid, ep, "txAba", parts);
    ASSERT_EQ(svc_->handleAddPartitions(reqBuf_.data(), n, now_, outBuf_.data(), outBuf_.size()), 0u);
    // Leadership moves away and comes back (a new epoch) between two duty cycles: the coordinator never sees it
    // lost, yet the answer it parked belongs to a leadership that no longer exists.
    replica_->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);
    replica_->becomeLeader(/*newEpoch=*/3, std::vector<rep::NodeId>{1, 2});
    followerFetches();
    (void)svc_->doWork(now_);
    ASSERT_EQ(answers_.frames.size(), 1u);
    const auto r = pc::AddPartitionsToTxnResponse::decode(answers_.frames[0].data(), answers_.frames[0].size());
    ASSERT_TRUE(r);
    ASSERT_EQ(r->results.size(), 1u);
    EXPECT_EQ(r->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::NotCoordinator))
        << "an answer parked under epoch 1 is never released by epoch 3's high watermark";
    EXPECT_EQ(svc_->parkedAnswers(), 0u);
}

TEST_F(TxnStateReplicationTest, AnAppendRefusedAfterAnUnnoticedDemotionIsNeverAnsweredOkAdversarialRegression) {
    int16_t ep = 0;
    const int64_t pid = initPid("txDemoted", ep);
    ASSERT_GE(pid, 0);
    // The replica is demoted, and the request arrives before the coordinator's next duty cycle notices.
    replica_->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);
    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    const std::size_t n = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), 1, ++corr_, pid, ep, "txDemoted", parts);
    const std::size_t out = svc_->handleAddPartitions(reqBuf_.data(), n, now_, outBuf_.data(), outBuf_.size());
    ASSERT_GT(out, 0u) << "a refused append is answered at once";
    const auto r = pc::AddPartitionsToTxnResponse::decode(outBuf_.data(), out);
    ASSERT_TRUE(r);
    ASSERT_EQ(r->results.size(), 1u);
    EXPECT_EQ(r->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::NotCoordinator))
        << "never an OK for a registration that was not appended";
    EXPECT_EQ(svc_->parkedAnswers(), 0u);
}

TEST_F(TxnStateReplicationTest, AnEndTxnPipelinedBehindAnUncommittedRegistrationWritesNoMarkerAndUnloadAnswersBothAdversarialRegression) {
    int markerAppends = 0;
    svc_->setAppendFnOverride([&](std::string_view, int32_t, std::span<const uint8_t>) -> int64_t {
        ++markerAppends;
        return 0;
    });
    int16_t ep = 0;
    const int64_t pid = initPid("txPipe", ep);
    ASSERT_GE(pid, 0);
    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    std::size_t n = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), 1, 201, pid, ep, "txPipe", parts);
    ASSERT_EQ(svc_->handleAddPartitions(reqBuf_.data(), n, now_, outBuf_.data(), outBuf_.size()), 0u);
    n = encodeEndTxn(reqBuf_.data(), reqBuf_.size(), 1, 202, pid, ep, /*commit=*/true, "txPipe");
    ASSERT_EQ(svc_->handleEndTxn(reqBuf_.data(), n, now_, outBuf_.data(), outBuf_.size()), 0u);
    for (int i = 0; i < 3; ++i) (void)svc_->doWork(now_ + 1 + i);
    EXPECT_EQ(markerAppends, 0) << "no COMMIT marker ahead of an uncommitted PrepareCommit";
    EXPECT_TRUE(answers_.frames.empty());
    isLeader_ = false;   // the coordinator moves before anything committed
    (void)svc_->doWork(now_ + 10);
    EXPECT_EQ(markerAppends, 0);
    ASSERT_EQ(answers_.frames.size(), 2u) << "both waiting answers are answered, none dropped";
    const auto add = pc::AddPartitionsToTxnResponse::decode(answers_.frames[0].data(), answers_.frames[0].size());
    const auto end = pc::EndTxnResponse::decode(answers_.frames[1].data(), answers_.frames[1].size());
    ASSERT_TRUE(add && end);
    ASSERT_EQ(add->results.size(), 1u);
    EXPECT_EQ(add->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
    EXPECT_EQ(end->correlationId, 202);
    EXPECT_EQ(end->errorCode, static_cast<int16_t>(pc::ErrorCode::NotCoordinator));
    EXPECT_EQ(svc_->parkedAnswers(), 0u);
    EXPECT_EQ(svc_->prepareGates(), 0u);
}

TEST_F(TxnStateReplicationTest, ACoordinatorRestartedOverAnUncommittedTailLoadsOnlyOnceItCommitsAdversarialRegression) {
    int16_t ep = 0;
    const int64_t pid = initPid("txRestart", ep);
    ASSERT_GE(pid, 0);
    const std::vector<pc::TxnTopicPartition> parts = {{0, 3}};
    std::size_t n = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), 1, ++corr_, pid, ep, "txRestart", parts);
    ASSERT_EQ(svc_->handleAddPartitions(reqBuf_.data(), n, now_, outBuf_.data(), outBuf_.size()), 0u);
    // Crash: the coordinator process state is gone, the log keeps the uncommitted registration.
    svc_ = std::make_unique<rune::node::TransactionCoordinationService>(
        txnStore_.get(), /*topics=*/nullptr, [this] { return isLeader_; }, [] { return rune::node::NodeId{1}; });
    svc_->setAnswerSinkForTest(&CapturedAnswers::sink, &answers_);
    (void)svc_->doWork(now_);
    n = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), 1, ++corr_, pid, ep, "txRestart", parts);
    std::size_t out = svc_->handleAddPartitions(reqBuf_.data(), n, now_, outBuf_.data(), outBuf_.size());
    ASSERT_GT(out, 0u);
    auto r = pc::AddPartitionsToTxnResponse::decode(outBuf_.data(), out);
    ASSERT_TRUE(r);
    ASSERT_EQ(r->results.size(), 1u);
    EXPECT_EQ(r->results[0].errorCode, static_cast<int16_t>(pc::ErrorCode::NotCoordinator))
        << "not loaded while the log holds an uncommitted tail";
    followerFetches();
    (void)svc_->doWork(now_);
    // Loaded from the committed log: the retry is answered (parked behind its own record, then released).
    n = encodeAddPartitions(reqBuf_.data(), reqBuf_.size(), 1, ++corr_, pid, ep, "txRestart", parts);
    out = svc_->handleAddPartitions(reqBuf_.data(), n, now_, outBuf_.data(), outBuf_.size());
    if (out == 0) {
        followerFetches();
        (void)svc_->doWork(now_);
        ASSERT_EQ(answers_.frames.size(), 1u);
        r = pc::AddPartitionsToTxnResponse::decode(answers_.frames[0].data(), answers_.frames[0].size());
    } else {
        r = pc::AddPartitionsToTxnResponse::decode(outBuf_.data(), out);
    }
    ASSERT_TRUE(r);
    ASSERT_EQ(r->results.size(), 1u);
    EXPECT_EQ(r->results[0].errorCode, 0) << "the registration survived the restart";
}

// ===========================================================================
// M1 (issues #125, #438, #359).  A transaction on the single-replica txn store (answers inline): init, one
// partition, AddOffsetsToTxn, then TxnOffsetCommit entries are sent by the test.
// ===========================================================================
namespace {
struct OffsetsTxn {
    std::string txn;
    std::string group;
    int64_t     pid   = -1;
    int16_t     epoch = 0;
};

OffsetsTxn beginOffsetsTxn(rune::node::TransactionCoordinationService& svc, uint8_t* req, uint8_t* out, std::size_t cap,
                           const std::string& txn, const std::string& group, int64_t nowNs) {
    OffsetsTxn t{txn, group};
    std::size_t n = encodeInitProducerId(req, cap, 1, 1, 30'000, txn);
    const auto init = pc::InitProducerIdResponse::decode(out, svc.handleInitProducerId(req, n, nowNs, out, cap));
    if (!init) return t;
    t.pid = init->producerId;
    t.epoch = init->producerEpoch;
    const std::vector<pc::TxnTopicPartition> parts = {{0, 0}};
    n = encodeAddPartitions(req, cap, 1, 2, t.pid, t.epoch, txn, parts);
    (void)svc.handleAddPartitions(req, n, nowNs, out, cap);
    pc::AddOffsetsToTxn add;
    add.sessionId = 1; add.correlationId = 3; add.producerId = t.pid; add.producerEpoch = t.epoch;
    add.transactionalId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(t.txn.data()), t.txn.size());
    add.groupId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(t.group.data()), t.group.size());
    n = add.encode(req);
    (void)svc.handleAddOffsets(req, n, nowNs, out, cap);
    return t;
}

/// TxnOffsetCommit of one entry (topic 0, partition 0); `generation < 0` sends no KIP-447 block (legacy).
int16_t txnOffsetCommit(rune::node::TransactionCoordinationService& svc, uint8_t* req, uint8_t* out, std::size_t cap,
                        const OffsetsTxn& t, int64_t offset, int64_t nowNs, int32_t generation = -1,
                        const std::string& member = {}) {
    pc::TxnOffsetCommit toc;
    toc.sessionId = 1; toc.correlationId = 4; toc.producerId = t.pid; toc.producerEpoch = t.epoch;
    toc.transactionalId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(t.txn.data()), t.txn.size());
    toc.groupId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(t.group.data()), t.group.size());
    toc.generationId = generation;
    toc.memberId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(member.data()), member.size());
    const std::vector<pc::TxnOffsetCommitEntry> entries = {{0, 0, offset, {}}};
    const std::size_t n = toc.encode(req, entries);
    const auto r = pc::TxnOffsetCommitResponse::decode(out, svc.handleTxnOffsetCommit(req, n, nowNs, out, cap));
    return r && !r->results.empty() ? r->results[0].errorCode : int16_t{-1};
}

struct MemberFence {
    std::string member = "m-1";
    int32_t     generation = 4;
    int         calls = 0;
    static pc::ErrorCode check(void* ctx, const std::string&, const std::string& member, int32_t generation, int64_t) {
        auto* f = static_cast<MemberFence*>(ctx);
        ++f->calls;
        if (member != f->member) return pc::ErrorCode::UnknownMember;
        return generation == f->generation ? pc::ErrorCode::OK : pc::ErrorCode::IllegalGeneration;
    }
};
}  // namespace

// Issue #125: TxnOffsetCommit carries the KIP-447 member id and generation and is fenced exactly as OffsetCommit.
TEST_F(TransactionCoordinationServiceTest, TxnOffsetCommitIsFencedByMemberAndGenerationRegression) {
    MemberFence fence;
    svc_->setTxnOffsetMemberFence(&MemberFence::check, &fence);
    const OffsetsTxn t = beginOffsetsTxn(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), "tx125", "cg125", 0);
    ASSERT_GE(t.pid, 0);
    EXPECT_EQ(txnOffsetCommit(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), t, 10, 0, 3, "m-1"),
              static_cast<int16_t>(pc::ErrorCode::IllegalGeneration)) << "a stale generation committed";
    EXPECT_EQ(txnOffsetCommit(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), t, 10, 0, 4, "m-other"),
              static_cast<int16_t>(pc::ErrorCode::UnknownMember)) << "a foreign member committed";
    EXPECT_TRUE(svc_->pendingOffsetsForTest("tx125").empty()) << "a fenced commit still added offsets";
    EXPECT_EQ(txnOffsetCommit(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), t, 10, 0, 4, "m-1"), 0);
    EXPECT_EQ(svc_->pendingOffsetsForTest("tx125").size(), 1u);
    const int before = fence.calls;
    EXPECT_EQ(txnOffsetCommit(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), t, 11, 0), 0)
        << "a legacy commit (no KIP-447 block) is accepted, as Kafka does";
    EXPECT_EQ(fence.calls, before) << "a legacy commit has no member to check";
}

// Issue #438: a (group, tp) is held by at most one unfinished transaction - another transaction's TxnOffsetCommit on
// it is answered ConcurrentTransactions (retriable) and adds nothing; the hold survives a coordinator reload (it is
// rebuilt from the durable txn state) and is released only once the holder's Complete* record is in the log.
TEST_F(TransactionCoordinationServiceTest, OneGroupPartitionIsHeldByOneTransactionAcrossAReloadRegression) {
    const OffsetsTxn a = beginOffsetsTxn(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), "tx-a", "cg438", 0);
    const OffsetsTxn b = beginOffsetsTxn(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), "tx-b", "cg438", 0);
    ASSERT_EQ(txnOffsetCommit(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), a, 42, 0), 0);
    const auto conflict = static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions);
    EXPECT_EQ(txnOffsetCommit(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), b, 50, 0), conflict);
    EXPECT_TRUE(svc_->pendingOffsetsForTest("tx-b").empty());
    const rune::coordination::TopicPartition k{"topic_0", 0};
    EXPECT_EQ(svc_->txnOffsetHold("cg438", k), rune::coordination::TxnOffsetHold::Held);

    // a reaches PrepareCommit; the coordinator reloads (a new service over the same durable txn store).
    std::size_t n = encodeEndTxn(reqBuf_.data(), reqBuf_.size(), 1, 5, a.pid, a.epoch, /*commit=*/true, a.txn);
    ASSERT_EQ(pc::EndTxnResponse::decode(outBuf_.data(), svc_->handleEndTxn(reqBuf_.data(), n, 0, outBuf_.data(),
                                                                            outBuf_.size()))->errorCode, 0);
    svc_ = std::make_unique<rune::node::TransactionCoordinationService>(
        txnStore_.get(), /*topics=*/nullptr, [this] { return isLeader_; }, [this] { return leaderNodeId_; });
    EXPECT_EQ(svc_->txnOffsetHold("cg438", k), rune::coordination::TxnOffsetHold::Unknown)
        << "before the load nothing is known: a fetch must not be answered";
    RefusingSink sink;
    sink.refusals = 1000;   // the group coordinator refuses: a stays PrepareCommit
    svc_->setTxnOffsetSink(&RefusingSink::apply, &sink);
    svc_->setAppendFnOverride([](std::string_view, int32_t, std::span<const uint8_t>) -> int64_t { return 0; });
    int64_t now = 1'000'000'000;
    (void)svc_->doWork(now);
    EXPECT_EQ(svc_->txnOffsetHold("cg438", k), rune::coordination::TxnOffsetHold::Held)
        << "the reload forgot that a PrepareCommit holds the key";
    EXPECT_EQ(txnOffsetCommit(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), b, 50, now), conflict)
        << "after the reload another transaction could commit the held key (the #438 rewind)";
}

// Issue #359: a commit stuck on a refused offset apply is visible - a gauge of stuck commits, the oldest one's age,
// and a periodic WARN - instead of a silent retry loop.
TEST_F(TransactionCoordinationServiceTest, AStuckOffsetApplyIsCountedAgedAndReportedRegression) {
    const OffsetsTxn t = beginOffsetsTxn(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), "tx359", "cg359", 0);
    ASSERT_EQ(txnOffsetCommit(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), t, 7, 0), 0);
    std::size_t n = encodeEndTxn(reqBuf_.data(), reqBuf_.size(), 1, 5, t.pid, t.epoch, /*commit=*/true, t.txn);
    ASSERT_EQ(pc::EndTxnResponse::decode(outBuf_.data(), svc_->handleEndTxn(reqBuf_.data(), n, 0, outBuf_.data(),
                                                                            outBuf_.size()))->errorCode, 0);
    svc_ = std::make_unique<rune::node::TransactionCoordinationService>(
        txnStore_.get(), /*topics=*/nullptr, [this] { return isLeader_; }, [this] { return leaderNodeId_; });
    RefusingSink sink;
    sink.refusals = 1'000'000;
    svc_->setTxnOffsetSink(&RefusingSink::apply, &sink);
    svc_->setAppendFnOverride([](std::string_view, int32_t, std::span<const uint8_t>) -> int64_t { return 0; });
    const int64_t start = 1'000'000'000;
    int64_t now = start;
    (void)svc_->doWork(now);
    svc_->onControlMarkerLanded(0, 0, t.pid, rune::gateway::ITxnRequestHandler::kAnyCoordinatorEpoch);
    EXPECT_EQ(svc_->txnOffsetCommitsStuck(), 1u);
    const uint64_t warnsAtStart = svc_->stuckTxnOffsetWarnsForTest();
    for (int i = 0; i < 70; ++i) { now += 1'000'000'000; (void)svc_->doWork(now); }   // 70 s of injected clock
    EXPECT_EQ(svc_->txnOffsetCommitsStuck(), 1u);
    EXPECT_GE(svc_->oldestStuckTxnOffsetAgeNs(now), int64_t{69'000'000'000}) << "the age of the oldest stuck commit";
    const uint64_t warns = svc_->stuckTxnOffsetWarnsForTest() - warnsAtStart;
    EXPECT_GE(warns, 2u) << "no periodic WARN while a commit is stuck";
    EXPECT_LE(warns, 3u) << "the WARN is rate-limited (once per 30 s)";
}

// ===========================================================================
// #438 over a two-replica __transaction_state: a hold is released only once the holder's Complete* record is
// COMMITTED - a reload while it is appended but not committed answers Unknown (the load waits for HW == LEO), and
// the committed state frees the key on both the old service and a fresh load.
// ===========================================================================
namespace {
struct RepTxn {
    rune::node::TransactionCoordinationService* svc = nullptr;
    std::function<void()> commitAll;   // the follower fetches + the service's doWork
    CapturedAnswers*      answers = nullptr;
    std::array<uint8_t, 1024> req{}, out{};
    int64_t corr = 100;
    /// The answer's first error code: inline, or (parked) once the records commit.
    template <class Resp>
    int16_t answer(std::size_t n) {
        if (n == 0) {
            commitAll();
            if (answers->frames.empty()) return -1;
            const auto f = answers->frames.back();
            answers->frames.clear();
            const auto r = Resp::decode(f.data(), f.size());
            return r ? r->errorCode : int16_t{-1};
        }
        const auto r = Resp::decode(out.data(), n);
        return r ? r->errorCode : int16_t{-1};
    }
    int16_t addOffsets(const std::string& txn, int64_t pid, int16_t ep, const std::string& group, int64_t now) {
        pc::AddOffsetsToTxn m;
        m.sessionId = 1; m.correlationId = ++corr; m.producerId = pid; m.producerEpoch = ep;
        m.transactionalId = {reinterpret_cast<const uint8_t*>(txn.data()), txn.size()};
        m.groupId = {reinterpret_cast<const uint8_t*>(group.data()), group.size()};
        return answer<pc::AddOffsetsToTxnResponse>(svc->handleAddOffsets(req.data(), m.encode(req.data()), now,
                                                                         out.data(), out.size()));
    }
    int16_t commit(const std::string& txn, int64_t pid, int16_t ep, const std::string& group, int64_t offset,
                   int64_t now) {
        pc::TxnOffsetCommit m;
        m.sessionId = 1; m.correlationId = ++corr; m.producerId = pid; m.producerEpoch = ep;
        m.transactionalId = {reinterpret_cast<const uint8_t*>(txn.data()), txn.size()};
        m.groupId = {reinterpret_cast<const uint8_t*>(group.data()), group.size()};
        const std::vector<pc::TxnOffsetCommitEntry> e = {{0, 0, offset, {}}};
        const std::size_t n = svc->handleTxnOffsetCommit(req.data(), m.encode(req.data(), e), now, out.data(), out.size());
        if (n == 0) {
            commitAll();
            if (answers->frames.empty()) return -1;
            const auto f = answers->frames.back();
            answers->frames.clear();
            const auto r = pc::TxnOffsetCommitResponse::decode(f.data(), f.size());
            return r && !r->results.empty() ? r->results[0].errorCode : int16_t{-1};
        }
        const auto r = pc::TxnOffsetCommitResponse::decode(out.data(), n);
        return r && !r->results.empty() ? r->results[0].errorCode : int16_t{-1};
    }
    int16_t endTxn(const std::string& txn, int64_t pid, int16_t ep, bool commitIt, int64_t now) {
        const std::size_t n = encodeEndTxn(req.data(), req.size(), 1, ++corr, pid, ep, commitIt, txn);
        return answer<pc::EndTxnResponse>(svc->handleEndTxn(req.data(), n, now, out.data(), out.size()));
    }
};
}  // namespace

TEST_F(TxnStateReplicationTest, AHoldOutlivesTheCompletionUntilItsCompleteRecordCommitsRegression) {
    using H = rune::coordination::TxnOffsetHold;
    const rune::coordination::TopicPartition key{"topic_0", 0};
    int16_t epA = 0, epB = 0;
    const int64_t pa = initPid("tx-a", epA);
    const int64_t pb = initPid("tx-b", epB);
    RepTxn t;
    t.svc = svc_.get();
    t.answers = &answers_;
    t.commitAll = [this, &t] { followerFetches(); (void)t.svc->doWork(now_); };
    ASSERT_EQ(t.addOffsets("tx-a", pa, epA, "cgh", now_), 0);
    ASSERT_EQ(t.commit("tx-a", pa, epA, "cgh", 42, now_), 0);
    ASSERT_EQ(t.addOffsets("tx-b", pb, epB, "cgh", now_), 0);
    EXPECT_EQ(svc_->txnOffsetHold("cgh", key), H::Held);
    EXPECT_EQ(svc_->txnOffsetHold("cgh", rune::coordination::TopicPartition{"", -1}), H::Held) << "any key of the group";
    EXPECT_EQ(svc_->txnOffsetHold("other", key), H::Free);
    EXPECT_EQ(t.commit("tx-b", pb, epB, "cgh", 50, now_), static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions));
    EXPECT_EQ(t.commit("tx-a", pa, epA, "cgh", 43, now_), 0) << "the holder itself commits the key again";
    ASSERT_EQ(t.endTxn("tx-a", pa, epA, /*commit=*/true, now_), 0);

    // a is PrepareCommit; a new coordinator load re-drives it (markers through the seam), and the marker lands.
    svc_ = std::make_unique<rune::node::TransactionCoordinationService>(
        txnStore_.get(), /*topics=*/nullptr, [this] { return isLeader_; }, [] { return rune::node::NodeId{1}; });
    svc_->setAnswerSinkForTest(&CapturedAnswers::sink, &answers_);
    svc_->setAppendFnOverride([](std::string_view, int32_t, std::span<const uint8_t>) -> int64_t { return 0; });
    t.svc = svc_.get();
    (void)svc_->doWork(now_);
    ASSERT_TRUE(svc_->isLoaded());
    EXPECT_EQ(svc_->txnOffsetHold("cgh", key), H::Held) << "the load forgot the PrepareCommit's key";
    svc_->onControlMarkerLanded(0, 0, pa, rune::gateway::ITxnRequestHandler::kAnyCoordinatorEpoch);
    ASSERT_EQ(svc_->txnStateForTest("tx-a"), tx::TxnState::CompleteCommit);
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnOffsetHold("cgh", key), H::Held)
        << "released before the CompleteCommit record committed (a reload could still re-drive a)";
    EXPECT_EQ(t.commit("tx-b", pb, epB, "cgh", 50, now_), static_cast<int16_t>(pc::ErrorCode::ConcurrentTransactions));

    // A reload NOW (CompleteCommit appended, not committed): nothing is known until the load can fold the log.
    auto fresh = std::make_unique<rune::node::TransactionCoordinationService>(
        txnStore_.get(), /*topics=*/nullptr, [this] { return isLeader_; }, [] { return rune::node::NodeId{1}; });
    (void)fresh->doWork(now_);
    EXPECT_FALSE(fresh->isLoaded());
    EXPECT_EQ(fresh->txnOffsetHold("cgh", key), H::Unknown);

    followerFetches();   // the CompleteCommit record commits
    (void)svc_->doWork(now_);
    EXPECT_EQ(svc_->txnOffsetHold("cgh", key), H::Free);
    (void)fresh->doWork(now_);
    ASSERT_TRUE(fresh->isLoaded());
    EXPECT_EQ(fresh->txnOffsetHold("cgh", key), H::Free) << "a committed CompleteCommit holds nothing after a load";
    EXPECT_EQ(t.commit("tx-b", pb, epB, "cgh", 50, now_), 0) << "the key is free for the next transaction";
    EXPECT_EQ(svc_->txnOffsetHold("cgh", key), H::Held);
}

// #125: a member block fenced with any code answers EVERY entry that code; a store-less rig (no fence) accepts it.
TEST_F(TransactionCoordinationServiceTest, AMemberFenceRefusalAnswersEveryEntryAndAddsNothingRegression) {
    struct Fence {
        pc::ErrorCode code = pc::ErrorCode::NotCoordinator;
        static pc::ErrorCode check(void* ctx, const std::string&, const std::string&, int32_t, int64_t) {
            return static_cast<Fence*>(ctx)->code;
        }
    } fence;
    const OffsetsTxn t = beginOffsetsTxn(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), "txf", "cgf", 0);
    ASSERT_GE(t.pid, 0);
    EXPECT_EQ(txnOffsetCommit(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), t, 3, 0, 0, "m"), 0)
        << "no fence wired: a block is accepted";
    svc_->setTxnOffsetMemberFence(&Fence::check, &fence);
    for (const pc::ErrorCode c : {pc::ErrorCode::NotCoordinator, pc::ErrorCode::UnknownMember,
                                  pc::ErrorCode::IllegalGeneration}) {
        fence.code = c;
        pc::TxnOffsetCommit m;
        m.sessionId = 1; m.correlationId = 9; m.producerId = t.pid; m.producerEpoch = t.epoch;
        m.transactionalId = {reinterpret_cast<const uint8_t*>(t.txn.data()), t.txn.size()};
        m.groupId = {reinterpret_cast<const uint8_t*>(t.group.data()), t.group.size()};
        m.generationId = 0;
        const std::vector<pc::TxnOffsetCommitEntry> e = {{0, 0, 7, {}}, {0, 1, 8, {}}};
        const auto r = pc::TxnOffsetCommitResponse::decode(
            outBuf_.data(), svc_->handleTxnOffsetCommit(reqBuf_.data(), m.encode(reqBuf_.data(), e), 0, outBuf_.data(),
                                                        outBuf_.size()));
        ASSERT_TRUE(r && r->results.size() == 2u);
        EXPECT_EQ(r->results[0].errorCode, static_cast<int16_t>(c));
        EXPECT_EQ(r->results[1].errorCode, static_cast<int16_t>(c));
        EXPECT_EQ(svc_->pendingOffsetsForTest("txf").size(), 1u) << "a fenced commit added offsets";
    }
}

// #359: the stuck gauge clears when the group coordinator takes the offsets; the sink's reason reaches the gauge
// publication (and the WARN); a Replicating answer is not "stuck".
TEST_F(TransactionCoordinationServiceTest, AStuckCommitClearsWhenTheOffsetsAreTakenAndPublishesItsGaugesRegression) {
    const OffsetsTxn t = beginOffsetsTxn(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), "tx359b", "cg359b", 0);
    ASSERT_EQ(txnOffsetCommit(*svc_, reqBuf_.data(), outBuf_.data(), outBuf_.size(), t, 7, 0), 0);
    std::size_t n = encodeEndTxn(reqBuf_.data(), reqBuf_.size(), 1, 5, t.pid, t.epoch, /*commit=*/true, t.txn);
    ASSERT_EQ(pc::EndTxnResponse::decode(outBuf_.data(), svc_->handleEndTxn(reqBuf_.data(), n, 0, outBuf_.data(),
                                                                            outBuf_.size()))->errorCode, 0);
    svc_ = std::make_unique<rune::node::TransactionCoordinationService>(
        txnStore_.get(), /*topics=*/nullptr, [this] { return isLeader_; }, [this] { return leaderNodeId_; });
    struct ReasonSink : RefusingSink {
        int reasonCalls = 0;
        static rune::coordination::TxnOffsetRefusal reason(void* ctx) {
            ++static_cast<ReasonSink*>(static_cast<RefusingSink*>(ctx))->reasonCalls;
            return rune::coordination::TxnOffsetRefusal::NotLoaded;
        }
    } sink;
    sink.refusals    = 5;
    sink.replicating = 2;
    svc_->setTxnOffsetSink(&RefusingSink::apply, static_cast<RefusingSink*>(&sink));
    svc_->setTxnOffsetRefusalReason(&ReasonSink::reason);
    const int& reasonCalls = sink.reasonCalls;
    struct Gauges {
        std::vector<std::pair<int64_t, int64_t>> seen;
        static bool sink(void* ctx, int64_t stuck, int64_t ageMs) {
            static_cast<Gauges*>(ctx)->seen.emplace_back(stuck, ageMs);
            return true;
        }
    } gauges;
    svc_->setStuckTxnOffsetsSink(&Gauges::sink, &gauges);
    svc_->setAppendFnOverride([](std::string_view, int32_t, std::span<const uint8_t>) -> int64_t { return 0; });
    int64_t now = 1'000'000'000;
    (void)svc_->doWork(now);
    svc_->onControlMarkerLanded(0, 0, t.pid, rune::gateway::ITxnRequestHandler::kAnyCoordinatorEpoch);
    EXPECT_EQ(svc_->txnOffsetCommitsStuck(), 1u);
    EXPECT_GE(reasonCalls, 1) << "the refusal reason was never asked for";
    bool sawStuck = false;
    for (int i = 0; i < 400 && svc_->txnStateForTest(t.txn) != tx::TxnState::CompleteCommit; ++i) {
        now += 100'000'000;
        (void)svc_->doWork(now);
        if (svc_->txnOffsetCommitsStuck() == 0 && sink.refusals > 0) ADD_FAILURE() << "cleared while still refused";
        for (const auto& g : gauges.seen) sawStuck = sawStuck || g.first == 1;
    }
    ASSERT_EQ(svc_->txnStateForTest(t.txn), tx::TxnState::CompleteCommit);
    EXPECT_TRUE(sawStuck) << "the stuck gauge was never published";
    EXPECT_EQ(svc_->txnOffsetCommitsStuck(), 0u);
    EXPECT_EQ(svc_->oldestStuckTxnOffsetAgeNs(now), 0);
    (void)svc_->doWork(now + 1);
    ASSERT_FALSE(gauges.seen.empty());
    EXPECT_EQ(gauges.seen.back(), (std::pair<int64_t, int64_t>{0, 0})) << "the gauges did not return to 0";
}
