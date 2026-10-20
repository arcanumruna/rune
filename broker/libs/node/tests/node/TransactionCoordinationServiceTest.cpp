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
    static bool apply(void* ctx, const std::string&, const rune::coordination::TopicPartition&, int64_t offset, int64_t) {
        auto* s = static_cast<RefusingSink*>(ctx);
        ++s->calls;
        if (s->refusals > 0) { --s->refusals; return false; }
        ++s->accepted;
        s->acceptedOffsets.push_back(offset);
        return true;
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
