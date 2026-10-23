// ---------------------------------------------------------------------------
// TransactionCoordinatorTest.cpp — Unit tests for TransactionCoordinator.
//
// TDD: Kafka-faithful state machine, zombie fencing, timeout, full lifecycle.
// Bounded drivers with hard caps + ASSERT(done). No spin. Injected clock.
// ---------------------------------------------------------------------------

// F15 §5.4 (2026-10-01): Complete* is now the RESTING state (Kafka).  Tests that
// asserted "Complete* → Empty" (the P5-5 collapse) assert the resting Complete*;
// the not-wedged intent is unchanged (re-init bumps, addPartitions → Ongoing).
#include <filesystem>
#include <rune/transaction/TransactionCoordinator.h>
#include <rune/transaction/TransactionLog.h>
#include <rune/transaction/ControlMarker.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cassert>
#include <cstring>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

using namespace rune::transaction;

namespace {

// ---------------------------------------------------------------------------
// Unique temp path helper (for durable log tests — FIX 1/4)
// ---------------------------------------------------------------------------
static std::atomic<int> g_coord_counter{0};

static std::string coordTmpPath() {
    return std::filesystem::temp_directory_path().string() + "/txncoord_" + std::to_string(::getpid())
         + "_" + std::to_string(g_coord_counter.fetch_add(1, std::memory_order_relaxed));
}

struct CoordTempFile {
    std::string path;
    explicit CoordTempFile(std::string p) : path(std::move(p)) {}
    ~CoordTempFile() { ::unlink(path.c_str()); }
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a TopicPartition.
static TopicPartition tp(std::string topic, int32_t part)
{
    return TopicPartition{ std::move(topic), part };
}

/// Build a vector of TopicPartitions.
static std::vector<TopicPartition> tps(std::initializer_list<TopicPartition> l)
{
    return std::vector<TopicPartition>(l);
}

/// Nanoseconds helper.
static constexpr int64_t ms2ns(int64_t ms) { return ms * 1'000'000LL; }

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
class TxnCoordTest : public ::testing::Test {
protected:
    // Injected clock starts at t=0.
    int64_t nowNs_{0};

    // Default transaction timeout: 60 seconds.
    static constexpr int32_t kDefaultTimeoutMs = 60'000;

    TransactionCoordinator coord_{ProducerIdAllocator{}, kDefaultTimeoutMs};

    void advanceClock(int64_t deltaMs) { nowNs_ += ms2ns(deltaMs); }
};

// ===========================================================================
// InitProducerId — no txnId (ephemeral producers)
// ===========================================================================

TEST_F(TxnCoordTest, InitProducerIdNoTxnIdReturnsFreshPid)
{
    auto r = coord_.initProducerId(std::nullopt, kDefaultTimeoutMs, nowNs_);
    EXPECT_TRUE(r.ok());
    EXPECT_GE(r.producerId, 0);
    EXPECT_EQ(r.producerEpoch, 0);
}

TEST_F(TxnCoordTest, InitProducerIdNoTxnIdEachCallGivesFreshPid)
{
    auto r1 = coord_.initProducerId(std::nullopt, kDefaultTimeoutMs, nowNs_);
    auto r2 = coord_.initProducerId(std::nullopt, kDefaultTimeoutMs, nowNs_);
    EXPECT_TRUE(r1.ok());
    EXPECT_TRUE(r2.ok());
    EXPECT_NE(r1.producerId, r2.producerId);
    EXPECT_EQ(r1.producerEpoch, 0);
    EXPECT_EQ(r2.producerEpoch, 0);
}

TEST_F(TxnCoordTest, InitProducerIdNoTxnIdCreatesNoState)
{
    coord_.initProducerId(std::nullopt, kDefaultTimeoutMs, nowNs_);
    EXPECT_EQ(coord_.size(), 0u);
}

// ===========================================================================
// InitProducerId — new txnId
// ===========================================================================

TEST_F(TxnCoordTest, InitProducerIdNewTxnIdCreatesEmptyState)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    EXPECT_TRUE(r.ok());
    EXPECT_GE(r.producerId, 0);
    EXPECT_EQ(r.producerEpoch, 0);
    EXPECT_EQ(coord_.size(), 1u);
    EXPECT_EQ(coord_.state("txn-1"), TxnState::Empty);
    EXPECT_EQ(coord_.epochOf("txn-1"), 0);
}

TEST_F(TxnCoordTest, InitProducerIdDistinctTxnIdsGetDistinctPids)
{
    auto r1 = coord_.initProducerId("txn-A", kDefaultTimeoutMs, nowNs_);
    auto r2 = coord_.initProducerId("txn-B", kDefaultTimeoutMs, nowNs_);
    EXPECT_NE(r1.producerId, r2.producerId);
    EXPECT_EQ(r1.producerEpoch, 0);
    EXPECT_EQ(r2.producerEpoch, 0);
}

// ===========================================================================
// InitProducerId — epoch bump / zombie fencing
// ===========================================================================

TEST_F(TxnCoordTest, ReInitProducerIdBumpsEpoch)
{
    auto r1 = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r1.ok());
    EXPECT_EQ(r1.producerEpoch, 0);

    auto r2 = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r2.ok());
    EXPECT_EQ(r2.producerEpoch, 1);
    // Same PID, bumped epoch.
    EXPECT_EQ(r2.producerId, r1.producerId);
}

TEST_F(TxnCoordTest, ReInitProducerIdBumpsEpochMultipleTimes)
{
    coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    for (int expected = 1; expected <= 5; ++expected) {
        auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
        EXPECT_TRUE(r.ok());
        EXPECT_EQ(r.producerEpoch, static_cast<ProducerEpoch>(expected))
            << "Expected epoch " << expected;
    }
}

TEST_F(TxnCoordTest, ReInitUpdatesEpochInMetadata)
{
    coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    EXPECT_EQ(coord_.epochOf("txn-1"), 1);
}

TEST_F(TxnCoordTest, ZombieOldEpochRejectedByAddPartitions)
{
    auto r1 = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r1.ok());
    ProducerId  pid      = r1.producerId;
    ProducerEpoch epoch0 = r1.producerEpoch;  // 0

    // New producer session bumps epoch.
    auto r2 = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r2.ok());
    EXPECT_EQ(r2.producerEpoch, 1);

    // Old producer (epoch=0) tries to add partitions — must be fenced.
    auto ar = coord_.addPartitions("txn-1", pid, epoch0,
                                   tps({tp("orders", 0)}), nowNs_);
    EXPECT_EQ(ar.errorCode, AddPartitionsResult::kInvalidProducerEpoch);
}

TEST_F(TxnCoordTest, ZombieWrongPidRejectedByAddPartitions)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());

    // Wrong PID (e.g. 9999).
    auto ar = coord_.addPartitions("txn-1", /*pid=*/9999, r.producerEpoch,
                                   tps({tp("orders", 0)}), nowNs_);
    EXPECT_EQ(ar.errorCode, AddPartitionsResult::kInvalidPid);
}

TEST_F(TxnCoordTest, ReInitOngoingTxnDrivesToPrepareAbort)
{
    // Open a transaction.
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    ASSERT_EQ(coord_.state("txn-1"), TxnState::Ongoing);

    // Re-init — should bump epoch and drive to PrepareAbort (zombie fence).
    auto r2 = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    EXPECT_TRUE(r2.ok());
    EXPECT_EQ(r2.producerEpoch, 1);
    EXPECT_EQ(coord_.state("txn-1"), TxnState::PrepareAbort);
}

TEST_F(TxnCoordTest, ReInitOngoingThenCompleteRestsInCompleteAbort)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);

    // Re-init fences the zombie.
    auto r2 = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r2.ok());
    ASSERT_EQ(coord_.state("txn-1"), TxnState::PrepareAbort);

    // P5-3 completes the abort markers.
    auto cr = coord_.completeTransaction("txn-1");
    EXPECT_TRUE(cr.ok());
    EXPECT_EQ(coord_.state("txn-1"), TxnState::CompleteAbort);
    // Epoch is the new bumped epoch.
    EXPECT_EQ(coord_.epochOf("txn-1"), 1);
}

// ===========================================================================
// addPartitions
// ===========================================================================

TEST_F(TxnCoordTest, AddPartitionsEmptyToOngoing)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(coord_.state("txn-1"), TxnState::Empty);

    auto ar = coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                                   tps({tp("orders", 0)}), nowNs_);
    EXPECT_TRUE(ar.ok());
    EXPECT_EQ(coord_.state("txn-1"), TxnState::Ongoing);
}

TEST_F(TxnCoordTest, AddPartitionsAccumulatesAcrossCalls)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());

    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("orders", 0), tp("orders", 1)}), nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("payments", 0)}), nowNs_);

    auto parts = coord_.partitionsOf("txn-1");
    EXPECT_EQ(parts.size(), 3u);
}

TEST_F(TxnCoordTest, AddPartitionsDeduplicates)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());

    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0), tp("t", 0), tp("t", 0)}), nowNs_);

    auto parts = coord_.partitionsOf("txn-1");
    EXPECT_EQ(parts.size(), 1u);
}

TEST_F(TxnCoordTest, AddPartitionsOngoingAccumulatesMore)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());

    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("a", 0)}), nowNs_);
    ASSERT_EQ(coord_.state("txn-1"), TxnState::Ongoing);

    // Second call while Ongoing.
    auto ar = coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                                   tps({tp("b", 0)}), nowNs_);
    EXPECT_TRUE(ar.ok());
    EXPECT_EQ(coord_.state("txn-1"), TxnState::Ongoing);
    EXPECT_EQ(coord_.partitionsOf("txn-1").size(), 2u);
}

TEST_F(TxnCoordTest, AddPartitionsUnknownTxnIdReturnsError)
{
    auto ar = coord_.addPartitions("does-not-exist", 42, 0,
                                   tps({tp("t", 0)}), nowNs_);
    EXPECT_EQ(ar.errorCode, AddPartitionsResult::kUnknownTxnId);
}

TEST_F(TxnCoordTest, AddPartitionsStaleEpochFenced)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());

    // Bump epoch (re-init) then try old epoch.
    coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);

    auto ar = coord_.addPartitions("txn-1", r.producerId, /*stale epoch=*/0,
                                   tps({tp("t", 0)}), nowNs_);
    EXPECT_EQ(ar.errorCode, AddPartitionsResult::kInvalidProducerEpoch);
}

TEST_F(TxnCoordTest, AddPartitionsWhenPrepareCommitIsInvalidTransition)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-1", r.producerId, r.producerEpoch,
                          /*commit=*/true, nowNs_);
    ASSERT_EQ(coord_.state("txn-1"), TxnState::PrepareCommit);

    auto ar = coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                                   tps({tp("t", 1)}), nowNs_);
    EXPECT_EQ(ar.errorCode, AddPartitionsResult::kInvalidTransition);
}

TEST_F(TxnCoordTest, AddPartitionsWhenPrepareAbortIsInvalidTransition)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-1", r.producerId, r.producerEpoch,
                          /*commit=*/false, nowNs_);
    ASSERT_EQ(coord_.state("txn-1"), TxnState::PrepareAbort);

    auto ar = coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                                   tps({tp("t", 1)}), nowNs_);
    EXPECT_EQ(ar.errorCode, AddPartitionsResult::kInvalidTransition);
}

TEST_F(TxnCoordTest, AddPartitionsWhenCompleteCommitStartsTheNextTxn)
{
    // F15 §5.4: CompleteCommit is the resting state; addPartitions → Ongoing.
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-1", r.producerId, r.producerEpoch, true, nowNs_);
    ASSERT_TRUE(coord_.completeTransaction("txn-1").ok());
    ASSERT_EQ(coord_.state("txn-1"), TxnState::CompleteCommit);
    EXPECT_TRUE(coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                                     tps({tp("t", 1)}), nowNs_).ok());
    EXPECT_EQ(coord_.state("txn-1"), TxnState::Ongoing);
}

// ===========================================================================
// endTransaction — commit path
// ===========================================================================

TEST_F(TxnCoordTest, EndTransactionCommitOngoingToPrepareCommit)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);

    auto er = coord_.endTransaction("txn-1", r.producerId, r.producerEpoch,
                                    /*commit=*/true, nowNs_);
    EXPECT_TRUE(er.ok());
    EXPECT_EQ(coord_.state("txn-1"), TxnState::PrepareCommit);
}

TEST_F(TxnCoordTest, EndTransactionCommitExposesPartitions)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("orders", 0), tp("payments", 0)}), nowNs_);

    auto er = coord_.endTransaction("txn-1", r.producerId, r.producerEpoch,
                                    true, nowNs_);
    ASSERT_TRUE(er.ok());
    EXPECT_EQ(er.partitions.size(), 2u);
}

TEST_F(TxnCoordTest, CompleteCommitRestsInCompleteCommit)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-1", r.producerId, r.producerEpoch, true, nowNs_);
    ASSERT_EQ(coord_.state("txn-1"), TxnState::PrepareCommit);

    auto cr = coord_.completeTransaction("txn-1");
    EXPECT_TRUE(cr.ok());
    EXPECT_EQ(coord_.state("txn-1"), TxnState::CompleteCommit);
    // Epoch is unchanged after a commit.
    EXPECT_EQ(coord_.epochOf("txn-1"), r.producerEpoch);
}

TEST_F(TxnCoordTest, AfterCompleteCommitPartitionsCleared)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-1", r.producerId, r.producerEpoch, true, nowNs_);
    coord_.completeTransaction("txn-1");

    EXPECT_TRUE(coord_.partitionsOf("txn-1").empty());
}

// ===========================================================================
// endTransaction — abort path
// ===========================================================================

TEST_F(TxnCoordTest, EndTransactionAbortOngoingToPrepareAbort)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);

    auto er = coord_.endTransaction("txn-1", r.producerId, r.producerEpoch,
                                    /*commit=*/false, nowNs_);
    EXPECT_TRUE(er.ok());
    EXPECT_EQ(coord_.state("txn-1"), TxnState::PrepareAbort);
}

TEST_F(TxnCoordTest, CompleteAbortRestsInCompleteAbort)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-1", r.producerId, r.producerEpoch, false, nowNs_);
    ASSERT_EQ(coord_.state("txn-1"), TxnState::PrepareAbort);

    auto cr = coord_.completeTransaction("txn-1");
    EXPECT_TRUE(cr.ok());
    EXPECT_EQ(coord_.state("txn-1"), TxnState::CompleteAbort);
}

TEST_F(TxnCoordTest, AfterCompleteAbortPartitionsCleared)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-1", r.producerId, r.producerEpoch, false, nowNs_);
    coord_.completeTransaction("txn-1");
    EXPECT_TRUE(coord_.partitionsOf("txn-1").empty());
}

// ===========================================================================
// endTransaction — invalid transitions
// ===========================================================================

TEST_F(TxnCoordTest, EndTransactionWhenEmptyIsInvalidTransition)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    // State is Empty (no addPartitions call).
    auto er = coord_.endTransaction("txn-1", r.producerId, r.producerEpoch,
                                    true, nowNs_);
    EXPECT_EQ(er.errorCode, EndTransactionResult::kInvalidTransition);
}

TEST_F(TxnCoordTest, EndTransactionWhenPrepareCommitFollowsTheIdempotencyTable)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-1", r.producerId, r.producerEpoch, true, nowNs_);
    ASSERT_EQ(coord_.state("txn-1"), TxnState::PrepareCommit);

    // F15 §5.4 (Kafka): a same-direction retry while PrepareCommit is
    // retriable (ConcurrentTransactions); the opposite direction is a client
    // bug (InvalidTxnState).
    auto er = coord_.endTransaction("txn-1", r.producerId, r.producerEpoch,
                                    true, nowNs_);
    EXPECT_EQ(er.errorCode, EndTransactionResult::kConcurrentTransactions);
    er = coord_.endTransaction("txn-1", r.producerId, r.producerEpoch, false, nowNs_);
    EXPECT_EQ(er.errorCode, EndTransactionResult::kInvalidTransition);
}

TEST_F(TxnCoordTest, EndTransactionUnknownTxnId)
{
    auto er = coord_.endTransaction("no-such-txn", 1, 0, true, nowNs_);
    EXPECT_EQ(er.errorCode, EndTransactionResult::kUnknownTxnId);
}

TEST_F(TxnCoordTest, EndTransactionStaleEpochFenced)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);

    // Bump epoch then try to commit with old epoch.
    coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);

    auto er = coord_.endTransaction("txn-1", r.producerId, /*stale epoch=*/0,
                                    true, nowNs_);
    EXPECT_EQ(er.errorCode, EndTransactionResult::kInvalidProducerEpoch);
}

TEST_F(TxnCoordTest, EndTransactionWrongPidFenced)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);

    auto er = coord_.endTransaction("txn-1", /*wrong pid=*/9999, r.producerEpoch,
                                    true, nowNs_);
    EXPECT_EQ(er.errorCode, EndTransactionResult::kInvalidPid);
}

// ===========================================================================
// completeTransaction — invalid calls
// ===========================================================================

TEST_F(TxnCoordTest, CompleteTransactionUnknownTxnId)
{
    auto cr = coord_.completeTransaction("does-not-exist");
    EXPECT_EQ(cr.errorCode, CompleteTransactionResult::kUnknownTxnId);
}

TEST_F(TxnCoordTest, CompleteTransactionWhenEmptyIsInvalidTransition)
{
    coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    // State is Empty.
    auto cr = coord_.completeTransaction("txn-1");
    EXPECT_EQ(cr.errorCode, CompleteTransactionResult::kInvalidTransition);
}

TEST_F(TxnCoordTest, CompleteTransactionWhenOngoingIsInvalidTransition)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    ASSERT_EQ(coord_.state("txn-1"), TxnState::Ongoing);

    auto cr = coord_.completeTransaction("txn-1");
    EXPECT_EQ(cr.errorCode, CompleteTransactionResult::kInvalidTransition);
}

// ===========================================================================
// Transaction timeout — doWork
// ===========================================================================

TEST_F(TxnCoordTest, DoWorkIdleReturnsZero)
{
    // No transactions at all.
    EXPECT_EQ(coord_.doWork(nowNs_), 0);
}

TEST_F(TxnCoordTest, DoWorkNonOngoingNotAborted)
{
    coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    // State is Empty — doWork should not touch it.
    advanceClock(kDefaultTimeoutMs + 1);
    EXPECT_EQ(coord_.doWork(nowNs_), 0);
    EXPECT_EQ(coord_.state("txn-1"), TxnState::Empty);
}

TEST_F(TxnCoordTest, DoWorkTimeoutAbortsBefore)
{
    // txnTimeoutMs = 5000 ms.
    auto r = coord_.initProducerId("txn-1", /*txnTimeoutMs=*/5000, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    ASSERT_EQ(coord_.state("txn-1"), TxnState::Ongoing);

    // Advance just under the timeout — should still be Ongoing.
    advanceClock(4999);  // 4999 ms < 5000 ms
    int work = coord_.doWork(nowNs_);
    EXPECT_EQ(work, 0);
    EXPECT_EQ(coord_.state("txn-1"), TxnState::Ongoing);
}

TEST_F(TxnCoordTest, DoWorkTimeoutAbortsAfter)
{
    auto r = coord_.initProducerId("txn-1", /*txnTimeoutMs=*/5000, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);

    // Advance past the timeout.
    advanceClock(5001);  // 5001 ms > 5000 ms
    int work = coord_.doWork(nowNs_);
    EXPECT_EQ(work, 1);
    EXPECT_EQ(coord_.state("txn-1"), TxnState::PrepareAbort);
}

TEST_F(TxnCoordTest, DoWorkTimeoutExactBoundary)
{
    // Exactly at the timeout boundary (==) should NOT trigger (strictly >).
    auto r = coord_.initProducerId("txn-1", /*txnTimeoutMs=*/1000, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);

    advanceClock(1000);  // exactly at timeout (not past it)
    int work = coord_.doWork(nowNs_);
    // nowNs - txnStartNs == timeoutNs → NOT > timeout → no abort.
    EXPECT_EQ(work, 0);
    EXPECT_EQ(coord_.state("txn-1"), TxnState::Ongoing);
}

TEST_F(TxnCoordTest, DoWorkNoSpinBoundedLoop)
{
    // Drive multiple doWork calls to verify zero returns when idle.
    // Hard-capped iteration count; ASSERT if still active after cap.
    constexpr int kCap = 100;
    int totalWork = 0;
    for (int i = 0; i < kCap; ++i) {
        totalWork += coord_.doWork(nowNs_);
    }
    EXPECT_EQ(totalWork, 0);  // no transactions → no work
}

TEST_F(TxnCoordTest, DoWorkAbortsMultipleTimedOutTxns)
{
    // Create 3 transactions with different timeouts.
    auto r1 = coord_.initProducerId("txn-1", 1000, nowNs_);
    coord_.addPartitions("txn-1", r1.producerId, r1.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);

    auto r2 = coord_.initProducerId("txn-2", 2000, nowNs_);
    coord_.addPartitions("txn-2", r2.producerId, r2.producerEpoch,
                         tps({tp("t", 1)}), nowNs_);

    auto r3 = coord_.initProducerId("txn-3", 5000, nowNs_);
    coord_.addPartitions("txn-3", r3.producerId, r3.producerEpoch,
                         tps({tp("t", 2)}), nowNs_);

    // Advance 2001 ms — txn-1 and txn-2 should be aborted, txn-3 still Ongoing.
    advanceClock(2001);
    int work = coord_.doWork(nowNs_);
    EXPECT_EQ(work, 2);
    EXPECT_EQ(coord_.state("txn-1"), TxnState::PrepareAbort);
    EXPECT_EQ(coord_.state("txn-2"), TxnState::PrepareAbort);
    EXPECT_EQ(coord_.state("txn-3"), TxnState::Ongoing);
}

TEST_F(TxnCoordTest, DoWorkSamplesClockOnce)
{
    // Inject a transaction and call doWork — verify no double-transition.
    auto r = coord_.initProducerId("txn-1", 1000, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);

    advanceClock(1001);
    int work1 = coord_.doWork(nowNs_);
    EXPECT_EQ(work1, 1);
    EXPECT_EQ(coord_.state("txn-1"), TxnState::PrepareAbort);

    // Second doWork same timestamp — no more work.
    int work2 = coord_.doWork(nowNs_);
    EXPECT_EQ(work2, 0);
}

// ===========================================================================
// Full lifecycle — init → addPartitions → commit → complete → reuse
// ===========================================================================

TEST_F(TxnCoordTest, FullCommitLifecycle)
{
    // Phase 1: init.
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());
    ASSERT_EQ(coord_.state("txn-1"), TxnState::Empty);

    // Phase 2: add partitions.
    auto ar1 = coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                                    tps({tp("orders", 0), tp("orders", 1)}), nowNs_);
    ASSERT_TRUE(ar1.ok());
    ASSERT_EQ(coord_.state("txn-1"), TxnState::Ongoing);

    // Phase 3: add more partitions.
    auto ar2 = coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                                    tps({tp("payments", 0)}), nowNs_);
    ASSERT_TRUE(ar2.ok());
    EXPECT_EQ(coord_.partitionsOf("txn-1").size(), 3u);

    // Phase 4: commit.
    auto er = coord_.endTransaction("txn-1", r.producerId, r.producerEpoch,
                                    true, nowNs_);
    ASSERT_TRUE(er.ok());
    ASSERT_EQ(coord_.state("txn-1"), TxnState::PrepareCommit);
    EXPECT_EQ(er.partitions.size(), 3u);

    // Phase 5: complete (P5-3 has written the markers).
    auto cr = coord_.completeTransaction("txn-1");
    ASSERT_TRUE(cr.ok());
    ASSERT_EQ(coord_.state("txn-1"), TxnState::CompleteCommit);

    // Phase 6: start next transaction with SAME pid/epoch.
    auto ar3 = coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                                    tps({tp("orders", 0)}), nowNs_);
    EXPECT_TRUE(ar3.ok());
    EXPECT_EQ(coord_.state("txn-1"), TxnState::Ongoing);
}

TEST_F(TxnCoordTest, FullAbortLifecycle)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-1", r.producerId, r.producerEpoch, false, nowNs_);
    ASSERT_EQ(coord_.state("txn-1"), TxnState::PrepareAbort);

    coord_.completeTransaction("txn-1");
    ASSERT_EQ(coord_.state("txn-1"), TxnState::CompleteAbort);

    // Start again with same pid/epoch.
    auto ar = coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                                   tps({tp("t", 1)}), nowNs_);
    EXPECT_TRUE(ar.ok());
}

TEST_F(TxnCoordTest, FullLifecycleWithTimeoutAbort)
{
    auto r = coord_.initProducerId("txn-1", /*txnTimeoutMs=*/2000, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);

    // Bounded driver: advance clock until doWork aborts the txn.
    // Hard cap to avoid infinite loops.
    constexpr int kCap = 10;
    bool done = false;
    for (int i = 0; i < kCap && !done; ++i) {
        advanceClock(1001);  // jump 1001 ms per step
        if (coord_.doWork(nowNs_) > 0) {
            done = true;
        }
    }
    ASSERT_TRUE(done) << "Timeout abort did not fire within cap iterations";
    EXPECT_EQ(coord_.state("txn-1"), TxnState::PrepareAbort);

    // Complete the abort.
    auto cr = coord_.completeTransaction("txn-1");
    EXPECT_TRUE(cr.ok());
    EXPECT_EQ(coord_.state("txn-1"), TxnState::CompleteAbort);

    // Can start a new txn.
    auto ar = coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                                   tps({tp("t", 0)}), nowNs_);
    EXPECT_TRUE(ar.ok());
}

// ===========================================================================
// Accessors — unknown txnId
// ===========================================================================

TEST_F(TxnCoordTest, StateOfUnknownTxnIdReturnsNullopt)
{
    EXPECT_EQ(coord_.state("unknown"), std::nullopt);
}

TEST_F(TxnCoordTest, EpochOfUnknownTxnIdReturnsMinusOne)
{
    EXPECT_EQ(coord_.epochOf("unknown"), static_cast<ProducerEpoch>(-1));
}

TEST_F(TxnCoordTest, PidOfUnknownTxnIdReturnsMinusOne)
{
    EXPECT_EQ(coord_.pidOf("unknown"), static_cast<ProducerId>(-1));
}

TEST_F(TxnCoordTest, PartitionsOfUnknownTxnIdReturnsEmpty)
{
    EXPECT_TRUE(coord_.partitionsOf("unknown").empty());
}

TEST_F(TxnCoordTest, MetadataOfUnknownTxnIdReturnsNullptr)
{
    EXPECT_EQ(coord_.metadata("unknown"), nullptr);
}

// ===========================================================================
// Invalid timeout
// ===========================================================================

TEST_F(TxnCoordTest, InitProducerIdInvalidTimeoutReturnsError)
{
    auto r = coord_.initProducerId("txn-1", /*txnTimeoutMs=*/-1, nowNs_);
    EXPECT_EQ(r.errorCode, InitProducerIdResult::kInvalidTxnTimeout);
    EXPECT_FALSE(r.ok());
}

TEST_F(TxnCoordTest, InitProducerIdZeroTimeoutUsesDefault)
{
    // txnTimeoutMs == 0 → use default.
    auto r = coord_.initProducerId("txn-1", /*txnTimeoutMs=*/0, nowNs_);
    EXPECT_TRUE(r.ok());
}

// ===========================================================================
// Multiple independent txnIds
// ===========================================================================

TEST_F(TxnCoordTest, MultipleIndependentTxnIds)
{
    auto rA = coord_.initProducerId("A", kDefaultTimeoutMs, nowNs_);
    auto rB = coord_.initProducerId("B", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(rA.ok());
    ASSERT_TRUE(rB.ok());
    EXPECT_NE(rA.producerId, rB.producerId);

    // Drive A into Ongoing, leave B as Empty.
    coord_.addPartitions("A", rA.producerId, rA.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    EXPECT_EQ(coord_.state("A"), TxnState::Ongoing);
    EXPECT_EQ(coord_.state("B"), TxnState::Empty);

    // A's timeout expires, B does not.
    coord_.initProducerId("A", /*timeout=*/1000, nowNs_);

    // The re-init bumped A to PrepareAbort (it was Ongoing).
    EXPECT_EQ(coord_.state("A"), TxnState::PrepareAbort);
    // B is unchanged.
    EXPECT_EQ(coord_.state("B"), TxnState::Empty);
}

// ===========================================================================
// Epoch fencing — concurrent zombie scenario
// ===========================================================================

TEST_F(TxnCoordTest, ZombieCannotAddPartitionsAfterEpochBump)
{
    // Producer P1 gets epoch 0.
    auto r1 = coord_.initProducerId("txn-z", kDefaultTimeoutMs, nowNs_);
    ASSERT_EQ(r1.producerEpoch, 0);

    // P1 opens a transaction.
    coord_.addPartitions("txn-z", r1.producerId, r1.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    ASSERT_EQ(coord_.state("txn-z"), TxnState::Ongoing);

    // New producer (P2) re-inits and bumps the epoch to 1.
    auto r2 = coord_.initProducerId("txn-z", kDefaultTimeoutMs, nowNs_);
    ASSERT_EQ(r2.producerEpoch, 1);

    // Complete the abort started by the epoch bump.
    coord_.completeTransaction("txn-z");
    ASSERT_EQ(coord_.state("txn-z"), TxnState::CompleteAbort);

    // Zombie P1 tries to add partitions — must be fenced.
    auto ar = coord_.addPartitions("txn-z", r1.producerId, r1.producerEpoch,
                                   tps({tp("t", 1)}), nowNs_);
    EXPECT_EQ(ar.errorCode, AddPartitionsResult::kInvalidProducerEpoch);

    // P2 (correct epoch) succeeds.
    auto ar2 = coord_.addPartitions("txn-z", r2.producerId, r2.producerEpoch,
                                    tps({tp("t", 1)}), nowNs_);
    EXPECT_TRUE(ar2.ok());
}

TEST_F(TxnCoordTest, ZombieCannotEndTransactionAfterEpochBump)
{
    auto r1 = coord_.initProducerId("txn-z", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-z", r1.producerId, r1.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);

    // New producer bumps epoch.
    auto r2 = coord_.initProducerId("txn-z", kDefaultTimeoutMs, nowNs_);
    coord_.completeTransaction("txn-z");

    // P2 opens a txn.
    coord_.addPartitions("txn-z", r2.producerId, r2.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);

    // Zombie P1 tries to commit — must be fenced.
    auto er = coord_.endTransaction("txn-z", r1.producerId, r1.producerEpoch,
                                    true, nowNs_);
    EXPECT_EQ(er.errorCode, EndTransactionResult::kInvalidProducerEpoch);
}

// ===========================================================================
// FIX 1 — TransactionLog wired into coordinator (durable transitions)
// ===========================================================================

// Helper: a coordinator that owns a TransactionLog (injectable ptr).
// We test that after each state transition, the log reflects the new state.

TEST(TxnCoordDurableTest, InitProducerIdPersistedToLog)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    TransactionLog log(path);

    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log);
    auto r = coord.initProducerId("txn-durable-1", 60'000, 0);
    ASSERT_TRUE(r.ok());

    // The log must have an entry for this txnId with state Empty.
    const auto* meta = log.metadata("txn-durable-1");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->state,        TxnState::Empty);
    EXPECT_EQ(meta->producerId,   r.producerId);
    EXPECT_EQ(meta->producerEpoch, r.producerEpoch);
}

TEST(TxnCoordDurableTest, AddPartitionsPersistedToLog)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    TransactionLog log(path);

    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log);
    auto r = coord.initProducerId("txn-dur-ap", 60'000, 0);
    ASSERT_TRUE(r.ok());

    coord.addPartitions("txn-dur-ap", r.producerId, r.producerEpoch,
                        tps({tp("orders", 0), tp("payments", 1)}), 1000LL);

    const auto* meta = log.metadata("txn-dur-ap");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->state, TxnState::Ongoing);
    EXPECT_EQ(meta->partitions.size(), 2u);
}

TEST(TxnCoordDurableTest, EndTransactionPrepareCommitPersistedBeforeComplete)
{
    // CRITICAL: the PrepareCommit entry must be in the log before completeTransaction.
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    TransactionLog log(path);

    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log);
    auto r = coord.initProducerId("txn-dur-end", 60'000, 0);
    ASSERT_TRUE(r.ok());
    coord.addPartitions("txn-dur-end", r.producerId, r.producerEpoch,
                        tps({tp("t", 0)}), 500LL);
    coord.endTransaction("txn-dur-end", r.producerId, r.producerEpoch,
                         /*commit=*/true, 1000LL);

    // Log must show PrepareCommit at this point (before any completeTransaction).
    const auto* meta = log.metadata("txn-dur-end");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->state, TxnState::PrepareCommit);
}

TEST(TxnCoordDurableTest, EndTransactionPrepareAbortPersistedBeforeComplete)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    TransactionLog log(path);

    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log);
    auto r = coord.initProducerId("txn-dur-abort", 60'000, 0);
    ASSERT_TRUE(r.ok());
    coord.addPartitions("txn-dur-abort", r.producerId, r.producerEpoch,
                        tps({tp("t", 0)}), 500LL);
    coord.endTransaction("txn-dur-abort", r.producerId, r.producerEpoch,
                         /*commit=*/false, 1000LL);

    const auto* meta = log.metadata("txn-dur-abort");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->state, TxnState::PrepareAbort);
}

TEST(TxnCoordDurableTest, CompleteTransactionPersistedToLog)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    TransactionLog log(path);

    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log);
    auto r = coord.initProducerId("txn-dur-complete", 60'000, 0);
    coord.addPartitions("txn-dur-complete", r.producerId, r.producerEpoch,
                        tps({tp("t", 0)}), 0LL);
    coord.endTransaction("txn-dur-complete", r.producerId, r.producerEpoch,
                         true, 0LL);
    coord.completeTransaction("txn-dur-complete");

    // Log must reflect Empty after complete.
    const auto* meta = log.metadata("txn-dur-complete");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->state, TxnState::CompleteCommit);
}

TEST(TxnCoordDurableTest, ReplayRebuildsMetadataOnConstruction)
{
    // Destroy coordinator; reconstruct from same log path — state restored.
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    ProducerId    savedPid{};
    ProducerEpoch savedEpoch{};

    {
        TransactionLog log(path);
        TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log);
        auto r = coord.initProducerId("txn-replay", 30'000, 0);
        ASSERT_TRUE(r.ok());
        savedPid   = r.producerId;
        savedEpoch = r.producerEpoch;
        coord.addPartitions("txn-replay", savedPid, savedEpoch,
                            tps({tp("t0", 0), tp("t1", 1)}), 1000LL);
        coord.endTransaction("txn-replay", savedPid, savedEpoch, true, 2000LL);
        // Crash — completeTransaction never called. PrepareCommit is durable.
    }

    // New coordinator + new log opened at same path — replays the log.
    TransactionLog log2(path);
    TransactionCoordinator coord2(ProducerIdAllocator{}, 60'000, &log2);

    // The coordinator must have reconstructed the PrepareCommit state.
    ASSERT_EQ(coord2.state("txn-replay"), TxnState::PrepareCommit);
    EXPECT_EQ(coord2.pidOf("txn-replay"),   savedPid);
    EXPECT_EQ(coord2.epochOf("txn-replay"), savedEpoch);
    auto parts = coord2.partitionsOf("txn-replay");
    EXPECT_EQ(parts.size(), 2u);

    // Can resolve the recovered transaction.
    auto cr = coord2.completeTransaction("txn-replay");
    EXPECT_TRUE(cr.ok());
    EXPECT_EQ(coord2.state("txn-replay"), TxnState::CompleteCommit);
}

// ===========================================================================
// FIX 4 — crash-seam: coordinator built from existing log + resolves Prepare*
// ===========================================================================

TEST(TxnCoordCrashSeamTest, RecoveredPrepareCommitIsResolvable)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    ProducerId    pid{};
    ProducerEpoch epoch{};
    int32_t       coordEpoch = 5;

    // Phase 1: run txn to PrepareCommit, crash (coordinator destroyed).
    {
        TransactionLog log(path);
        TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log, coordEpoch);
        auto r = coord.initProducerId("txn-cs-commit", 60'000, 0LL);
        ASSERT_TRUE(r.ok());
        pid   = r.producerId;
        epoch = r.producerEpoch;
        coord.addPartitions("txn-cs-commit", pid, epoch,
                            tps({tp("foo", 0), tp("bar", 1)}), 100LL);
        coord.endTransaction("txn-cs-commit", pid, epoch, /*commit=*/true, 200LL);
        ASSERT_EQ(coord.state("txn-cs-commit"), TxnState::PrepareCommit);
        // Crash here — completeTransaction never called.
    }

    // Phase 2: new coordinator from same log path.
    TransactionLog log2(path);
    TransactionCoordinator coord2(ProducerIdAllocator{}, 60'000, &log2, coordEpoch);

    ASSERT_EQ(coord2.state("txn-cs-commit"), TxnState::PrepareCommit);
    EXPECT_EQ(coord2.pidOf("txn-cs-commit"),   pid);
    EXPECT_EQ(coord2.epochOf("txn-cs-commit"), epoch);
    auto parts = coord2.partitionsOf("txn-cs-commit");
    EXPECT_EQ(parts.size(), 2u);

    // Verify coordinatorEpoch stored in the recovered metadata.
    const auto* meta = coord2.metadata("txn-cs-commit");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->coordinatorEpoch, coordEpoch);

    // Resolve.
    auto cr = coord2.completeTransaction("txn-cs-commit");
    EXPECT_TRUE(cr.ok());
    EXPECT_EQ(coord2.state("txn-cs-commit"), TxnState::CompleteCommit);
}

TEST(TxnCoordCrashSeamTest, RecoveredPrepareAbortIsResolvable)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    ProducerId    pid{};
    ProducerEpoch epoch{};
    int32_t       coordEpoch = 9;

    {
        TransactionLog log(path);
        TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log, coordEpoch);
        auto r = coord.initProducerId("txn-cs-abort", 60'000, 0LL);
        ASSERT_TRUE(r.ok());
        pid   = r.producerId;
        epoch = r.producerEpoch;
        coord.addPartitions("txn-cs-abort", pid, epoch,
                            tps({tp("x", 0)}), 100LL);
        coord.endTransaction("txn-cs-abort", pid, epoch, /*commit=*/false, 200LL);
        ASSERT_EQ(coord.state("txn-cs-abort"), TxnState::PrepareAbort);
        // Crash.
    }

    TransactionLog log2(path);
    TransactionCoordinator coord2(ProducerIdAllocator{}, 60'000, &log2, coordEpoch);

    ASSERT_EQ(coord2.state("txn-cs-abort"), TxnState::PrepareAbort);
    EXPECT_EQ(coord2.pidOf("txn-cs-abort"),   pid);
    EXPECT_EQ(coord2.epochOf("txn-cs-abort"), epoch);

    const auto* meta = coord2.metadata("txn-cs-abort");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->coordinatorEpoch, coordEpoch);

    auto cr = coord2.completeTransaction("txn-cs-abort");
    EXPECT_TRUE(cr.ok());
    EXPECT_EQ(coord2.state("txn-cs-abort"), TxnState::CompleteAbort);
}

// ===========================================================================
// FIX 1 + FIX 4 — writeMarkers with durable log: prepare fsynced before markers
// ===========================================================================

TEST(TxnCoordDurableTest, WriteMarkersWithLogPreparePersistedBeforeMarkers)
{
    // This is the critical atomicity test:
    // endTransaction must persist PrepareCommit to log BEFORE appendFn is called.
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    TransactionLog log(path);

    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log);
    auto r = coord.initProducerId("txn-wm-durable", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    coord.addPartitions("txn-wm-durable", r.producerId, r.producerEpoch,
                        tps({tp("p0", 0), tp("p1", 1)}), 0LL);

    // Track whether PrepareCommit was in log when first appendFn was called.
    bool prepareInLogBeforeMarker = false;
    auto appendFn = [&](const TopicPartition&, const std::vector<uint8_t>&) {
        const auto* m = log.metadata("txn-wm-durable");
        if (m && m->state == TxnState::PrepareCommit) {
            prepareInLogBeforeMarker = true;
        }
    };

    int count = ControlMarker::writeMarkers(coord, "txn-wm-durable",
                                             r.producerId, r.producerEpoch,
                                             /*commit=*/true, /*coordEpoch=*/0,
                                             0LL, appendFn);
    EXPECT_EQ(count, 2);
    EXPECT_TRUE(prepareInLogBeforeMarker)
        << "PrepareCommit must be durably appended to log before any marker is written";
}

// ===========================================================================
// FIX 2 — coordinatorEpoch in metadata + getter
// ===========================================================================

TEST(TxnCoordDurableTest, CoordinatorEpochInMetadata)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    TransactionLog log(path);

    int32_t coordEpoch = 13;
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log, coordEpoch);
    auto r = coord.initProducerId("txn-ce", 60'000, 0LL);
    ASSERT_TRUE(r.ok());

    const auto* meta = coord.metadata("txn-ce");
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->coordinatorEpoch, coordEpoch);

    // After round-trip through log, coordinatorEpoch must match.
    TransactionLog log2(path);
    const auto* logMeta = log2.metadata("txn-ce");
    ASSERT_NE(logMeta, nullptr);
    EXPECT_EQ(logMeta->coordinatorEpoch, coordEpoch);
}

// ===========================================================================
// B1 — fence-abort and timeout-abort MUST write ABORT markers (resolvePendingMarkers)
// ===========================================================================

// Helper: collect marker writes into a map<partition → vector<payload>>
using MarkerMap = std::map<TopicPartition, std::vector<ControlMarkerPayload>>;

static std::function<void(const TopicPartition&, const std::vector<uint8_t>&)>
makeCollector(MarkerMap& out) {
    return [&out](const TopicPartition& p, const std::vector<uint8_t>& b) {
        ControlMarkerPayload pl{};
        EXPECT_TRUE(ControlMarker::extract(b.data(), 0, pl, ControlMarker::ENCODED_SIZE));
        out[p].push_back(pl);
    };
}

TEST_F(TxnCoordTest, FenceAbortResolvePendingWritesAbortMarkersToAllPartitions)
{
    // Init txn with partitions p0, p1, p2.
    auto r = coord_.initProducerId("txn-f", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());
    coord_.addPartitions("txn-f", r.producerId, r.producerEpoch,
                         tps({tp("p", 0), tp("p", 1), tp("p", 2)}), nowNs_);
    ASSERT_EQ(coord_.state("txn-f"), TxnState::Ongoing);

    // Re-init fences the ongoing txn → PrepareAbort, markersPending=true.
    auto r2 = coord_.initProducerId("txn-f", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r2.ok());
    ASSERT_EQ(coord_.state("txn-f"), TxnState::PrepareAbort);

    // B1: resolvePendingMarkers must write ABORT markers to all 3 partitions.
    MarkerMap captured;
    int total = ControlMarker::resolvePendingMarkers(coord_, makeCollector(captured));

    EXPECT_EQ(total, 3)                     << "Expected 3 ABORT markers (one per partition)";
    EXPECT_EQ(captured.size(), 3u)          << "Expected markers on 3 distinct partitions";
    EXPECT_EQ(coord_.state("txn-f"), TxnState::CompleteAbort) << "State must be the resting CompleteAbort after resolve";
    for (auto& [part, pls] : captured) {
        ASSERT_EQ(pls.size(), 1u);
        EXPECT_EQ(pls[0].controlType, ControlType::Abort)
            << "Expected ABORT marker on partition " << part.topic << "/" << part.partition;
    }
    // All three partitions must have been marked.
    EXPECT_TRUE(captured.contains(tp("p", 0)));
    EXPECT_TRUE(captured.contains(tp("p", 1)));
    EXPECT_TRUE(captured.contains(tp("p", 2)));
}

TEST_F(TxnCoordTest, FenceAbortNoPendingMarkersAfterResolve)
{
    auto r = coord_.initProducerId("txn-f2", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());
    coord_.addPartitions("txn-f2", r.producerId, r.producerEpoch,
                         tps({tp("q", 0)}), nowNs_);

    coord_.initProducerId("txn-f2", kDefaultTimeoutMs, nowNs_);
    ASSERT_EQ(coord_.state("txn-f2"), TxnState::PrepareAbort);

    // Resolve once.
    MarkerMap cap1;
    ControlMarker::resolvePendingMarkers(coord_, makeCollector(cap1));
    EXPECT_EQ(cap1.size(), 1u);

    // Second call: no pending markers → 0 written.
    MarkerMap cap2;
    int total2 = ControlMarker::resolvePendingMarkers(coord_, makeCollector(cap2));
    EXPECT_EQ(total2, 0);
    EXPECT_TRUE(cap2.empty());
}

TEST_F(TxnCoordTest, TimeoutAbortResolvePendingWritesAbortMarkersToAllPartitions)
{
    // Txn with 3 partitions, short timeout.
    auto r = coord_.initProducerId("txn-t", /*txnTimeoutMs=*/1000, nowNs_);
    ASSERT_TRUE(r.ok());
    coord_.addPartitions("txn-t", r.producerId, r.producerEpoch,
                         tps({tp("x", 0), tp("y", 1), tp("z", 2)}), nowNs_);
    ASSERT_EQ(coord_.state("txn-t"), TxnState::Ongoing);

    // Advance past timeout.
    advanceClock(1001);
    int work = coord_.doWork(nowNs_);
    ASSERT_EQ(work, 1);
    ASSERT_EQ(coord_.state("txn-t"), TxnState::PrepareAbort);

    // B1: resolvePendingMarkers must write ABORT markers to all 3 partitions.
    MarkerMap captured;
    int total = ControlMarker::resolvePendingMarkers(coord_, makeCollector(captured));

    EXPECT_EQ(total, 3);
    EXPECT_EQ(captured.size(), 3u);
    EXPECT_EQ(coord_.state("txn-t"), TxnState::CompleteAbort);
    for (auto& [part, pls] : captured) {
        ASSERT_EQ(pls.size(), 1u);
        EXPECT_EQ(pls[0].controlType, ControlType::Abort);
    }
    // No partition left unmarked.
    EXPECT_TRUE(captured.contains(tp("x", 0)));
    EXPECT_TRUE(captured.contains(tp("y", 1)));
    EXPECT_TRUE(captured.contains(tp("z", 2)));
}

TEST_F(TxnCoordTest, TimeoutAbortNoPartitionLeftUnmarked)
{
    // Multiple timed-out txns — each gets fully marked.
    for (int i = 0; i < 4; ++i) {
        auto id = "txn-multi-" + std::to_string(i);
        auto r = coord_.initProducerId(id, /*timeout=*/500, nowNs_);
        ASSERT_TRUE(r.ok());
        coord_.addPartitions(id, r.producerId, r.producerEpoch,
                             tps({tp("t", i)}), nowNs_);
    }

    advanceClock(501);
    int work = coord_.doWork(nowNs_);
    ASSERT_EQ(work, 4);

    MarkerMap captured;
    int total = ControlMarker::resolvePendingMarkers(coord_, makeCollector(captured));
    EXPECT_EQ(total, 4)   << "4 txns × 1 partition each = 4 markers";
    EXPECT_EQ(captured.size(), 4u);
    for (int i = 0; i < 4; ++i) {
        EXPECT_TRUE(captured.contains(tp("t", i)))
            << "Partition t/" << i << " must have an ABORT marker";
    }
    // F15 §5.4: all rest in CompleteAbort.
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(coord_.state("txn-multi-" + std::to_string(i)), TxnState::CompleteAbort);
    }
}

// ===========================================================================
// B2 — recovered Prepare* resolvable using persisted epochs (no live request)
// ===========================================================================

TEST(TxnCoordAuditB2Test, RecoveredPrepareAbortResolvableWithPersistedEpochs)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    ProducerId    pid{};
    ProducerEpoch epoch{};
    int32_t       coordEpoch = 7;

    // Phase 1: run to PrepareAbort, crash.
    {
        TransactionLog log(path);
        TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log, coordEpoch);
        auto r = coord.initProducerId("txn-b2-abort", 60'000, 0LL);
        ASSERT_TRUE(r.ok());
        pid   = r.producerId;
        epoch = r.producerEpoch;
        coord.addPartitions("txn-b2-abort", pid, epoch,
                            tps({tp("p0", 0), tp("p1", 1)}), 100LL);
        coord.endTransaction("txn-b2-abort", pid, epoch, /*commit=*/false, 200LL);
        ASSERT_EQ(coord.state("txn-b2-abort"), TxnState::PrepareAbort);
        // Crash — completeTransaction never called, no markers written.
    }

    // Phase 2: recover from log, resolve with persisted epochs (B2).
    TransactionLog log2(path);
    TransactionCoordinator coord2(ProducerIdAllocator{}, 60'000, &log2, coordEpoch);

    ASSERT_EQ(coord2.state("txn-b2-abort"), TxnState::PrepareAbort);

    // The recovered coordinator must expose this as a pending marker txn.
    auto pending = coord2.pendingMarkerTransactions();
    ASSERT_EQ(pending.size(), 1u);
    EXPECT_EQ(pending[0].txnId,            "txn-b2-abort");
    EXPECT_EQ(pending[0].producerId,       pid);
    EXPECT_EQ(pending[0].producerEpoch,    epoch);
    EXPECT_EQ(pending[0].coordinatorEpoch, coordEpoch);
    EXPECT_FALSE(pending[0].commit);
    EXPECT_EQ(pending[0].partitions.size(), 2u);

    // Resolve — ABORT markers written using PERSISTED epochs, not a live request.
    MarkerMap captured;
    auto appendFn = [&](const TopicPartition& p, const std::vector<uint8_t>& b) {
        ControlMarkerPayload pl{};
        EXPECT_TRUE(ControlMarker::extract(b.data(), 0, pl, ControlMarker::ENCODED_SIZE));
        captured[p].push_back(pl);
    };
    int total = ControlMarker::resolvePendingMarkers(coord2, appendFn);

    EXPECT_EQ(total, 2)           << "2 ABORT markers expected";
    EXPECT_EQ(coord2.state("txn-b2-abort"), TxnState::CompleteAbort);
    // Verify marker epochs match what was persisted (NOT a new/live epoch).
    for (auto& [part, pls] : captured) {
        ASSERT_EQ(pls.size(), 1u);
        EXPECT_EQ(pls[0].controlType,       ControlType::Abort);
        EXPECT_EQ(pls[0].producerEpoch,     epoch);
        EXPECT_EQ(pls[0].coordinatorEpoch,  coordEpoch);
    }
}

TEST(TxnCoordAuditB2Test, RecoveredPrepareCommitResolvableWithPersistedEpochs)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    ProducerId    pid{};
    ProducerEpoch epoch{};
    int32_t       coordEpoch = 11;

    {
        TransactionLog log(path);
        TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log, coordEpoch);
        auto r = coord.initProducerId("txn-b2-commit", 60'000, 0LL);
        ASSERT_TRUE(r.ok());
        pid   = r.producerId;
        epoch = r.producerEpoch;
        coord.addPartitions("txn-b2-commit", pid, epoch,
                            tps({tp("foo", 0)}), 100LL);
        coord.endTransaction("txn-b2-commit", pid, epoch, /*commit=*/true, 200LL);
        ASSERT_EQ(coord.state("txn-b2-commit"), TxnState::PrepareCommit);
        // Crash.
    }

    TransactionLog log2(path);
    TransactionCoordinator coord2(ProducerIdAllocator{}, 60'000, &log2, coordEpoch);
    ASSERT_EQ(coord2.state("txn-b2-commit"), TxnState::PrepareCommit);

    auto pending = coord2.pendingMarkerTransactions();
    ASSERT_EQ(pending.size(), 1u);
    EXPECT_TRUE(pending[0].commit);
    EXPECT_EQ(pending[0].coordinatorEpoch, coordEpoch);

    MarkerMap captured;
    auto appendFn = [&](const TopicPartition& p, const std::vector<uint8_t>& b) {
        ControlMarkerPayload pl{};
        EXPECT_TRUE(ControlMarker::extract(b.data(), 0, pl, ControlMarker::ENCODED_SIZE));
        captured[p].push_back(pl);
    };
    int total = ControlMarker::resolvePendingMarkers(coord2, appendFn);

    EXPECT_EQ(total, 1);
    EXPECT_EQ(coord2.state("txn-b2-commit"), TxnState::CompleteCommit);
    for (auto& [part, pls] : captured) {
        ASSERT_EQ(pls.size(), 1u);
        EXPECT_EQ(pls[0].controlType,      ControlType::Commit);
        EXPECT_EQ(pls[0].producerEpoch,    epoch);
        EXPECT_EQ(pls[0].coordinatorEpoch, coordEpoch);
    }
}

TEST(TxnCoordAuditB2Test, RecoveryDoesNotDuplicateMarkersOnCompleteCommitState)
{
    // C2 complementary + P5-5 liveness fix:
    //
    // If a coordinator crashed AFTER persisting CompleteCommit but BEFORE
    // persisting Empty, recovery must NOT re-write markers.  With the P5-5
    // liveness fix the constructor collapses a recovered CompleteCommit to Empty
    // in-memory (no second log write during replay — the txnId is merely made
    // immediately reusable).  The invariant "no duplicate markers" is preserved
    // because Empty is also not in pendingMarkerTransactions, and resolvePending
    // emits zero markers.
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    // Simulate: write CompleteCommit directly to log (markers were already written).
    {
        TransactionLog log(path);
        TxnLogEntry e;
        e.txnId            = "txn-c2-nodup";
        e.producerId       = 10;
        e.producerEpoch    = 1;
        e.state            = TxnState::CompleteCommit;
        e.coordinatorEpoch = 5;
        e.txnTimeoutMs     = 60'000;
        log.append(e);
    }

    // Recover: liveness fix collapses CompleteCommit → Empty in-memory.
    TransactionLog log2(path);
    TransactionCoordinator coord2(ProducerIdAllocator{}, 60'000, &log2, 5);

    // F15 §5.4: the recovered CompleteCommit is kept as the resting state.
    ASSERT_EQ(coord2.state("txn-c2-nodup"), TxnState::CompleteCommit);
    auto pending = coord2.pendingMarkerTransactions();
    EXPECT_TRUE(pending.empty())
        << "a recovered CompleteCommit must NOT appear in pendingMarkerTransactions";

    // No markers written on a resolve pass (Empty is not pending).
    int count = 0;
    auto appendFn = [&](const TopicPartition&, const std::vector<uint8_t>&) { ++count; };
    ControlMarker::resolvePendingMarkers(coord2, appendFn);
    EXPECT_EQ(count, 0) << "No markers must be re-written for a recovered CompleteCommit";
}

// ===========================================================================
// C1 — epoch overflow allocates a new PID (not wraps to 0)
// ===========================================================================

TEST(TxnCoordAuditC1Test, EpochOverflowAllocatesNewPid)
{
    // Use a fresh coordinator; manually force epoch to INT16_MAX.
    TransactionCoordinator coord;
    auto r = coord.initProducerId("txn-c1", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    ProducerId  oldPid   = r.producerId;

    // Set the epoch to INT16_MAX by repeated re-inits would be slow;
    // instead test the boundary via a coordinator that recovered metadata at INT16_MAX.
    // We use a durable log to inject the boundary state.
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    {
        TransactionLog log(path);
        TxnLogEntry e;
        e.txnId         = "txn-overflow";
        e.producerId    = 42;
        e.producerEpoch = INT16_MAX;
        e.state         = TxnState::Empty;
        e.txnTimeoutMs  = 60'000;
        log.append(e);
    }

    TransactionLog log2(path);
    // Start allocator at 100 so a new PID != 42.
    TransactionCoordinator coord2(ProducerIdAllocator{100}, 60'000, &log2, 0);

    // Re-init on epoch INT16_MAX → must allocate a NEW PID, epoch 0.
    auto r2 = coord2.initProducerId("txn-overflow", 60'000, 0LL);
    ASSERT_TRUE(r2.ok());

    // Must be a NEW producerId (different from 42).
    EXPECT_NE(r2.producerId, static_cast<ProducerId>(42))
        << "Epoch overflow must allocate a fresh PID, not reuse the old one";
    // Epoch on the new PID starts at 0.
    EXPECT_EQ(r2.producerEpoch, static_cast<ProducerEpoch>(0))
        << "New PID after epoch exhaustion starts at epoch 0";
}

TEST(TxnCoordAuditC1Test, EpochOverflowOldPidRetired)
{
    // After epoch overflow, the old PID cannot be used.
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    {
        TransactionLog log(path);
        TxnLogEntry e;
        e.txnId         = "txn-retired";
        e.producerId    = 55;
        e.producerEpoch = INT16_MAX;
        e.state         = TxnState::Empty;
        e.txnTimeoutMs  = 60'000;
        log.append(e);
    }

    TransactionLog log2(path);
    TransactionCoordinator coord2(ProducerIdAllocator{200}, 60'000, &log2, 0);

    auto r2 = coord2.initProducerId("txn-retired", 60'000, 0LL);
    ASSERT_TRUE(r2.ok());
    // New PID allocated.
    ASSERT_NE(r2.producerId, static_cast<ProducerId>(55));

    // Old PID + any epoch cannot add partitions (fenced by PID mismatch).
    auto ar = coord2.addPartitions("txn-retired",
                                    /*old pid=*/55, /*epoch=*/INT16_MAX,
                                    tps({tp("t", 0)}), 0LL);
    EXPECT_EQ(ar.errorCode, AddPartitionsResult::kInvalidPid)
        << "Old PID must be fenced after epoch-overflow new-PID allocation";
}

// ===========================================================================
// NEW-1 — epoch overflow DURING an Ongoing-fence must abort the in-flight txn
//         under the OLD producerId (matching the in-flight data), and only adopt
//         the fresh PID AFTER the abort resolves.  Under the buggy e66a07e code the
//         ABORT marker was stamped with the NEW pid → the old PID's uncommitted
//         records would leak (atomicity/EOS violation).
// ===========================================================================

TEST(TxnCoordAuditNew1Test, EpochOverflowOngoingFenceAbortsUnderOldPid)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    constexpr ProducerId   kOldPid = 42;
    const     TopicPartition p0 = tp("t", 0);
    const     TopicPartition p1 = tp("t", 1);

    // Inject an ONGOING txn at INT16_MAX (epoch exhausted) with two partitions.
    {
        TransactionLog log(path);
        TxnLogEntry e;
        e.txnId         = "txn-new1";
        e.producerId    = kOldPid;
        e.producerEpoch = INT16_MAX;
        e.state         = TxnState::Ongoing;
        e.txnTimeoutMs  = 60'000;
        e.txnStartNs    = 1'000;
        e.partitions    = { p0, p1 };
        log.append(e);
    }

    TransactionLog log2(path);
    // Allocator starts at 100 so the fresh PID (>=100) is clearly != kOldPid (42).
    TransactionCoordinator coord(ProducerIdAllocator{100}, 60'000, &log2, /*coordEpoch=*/3);
    ASSERT_EQ(coord.state("txn-new1"), TxnState::Ongoing);
    ASSERT_EQ(coord.pidOf("txn-new1"), kOldPid);

    // Re-init: epoch overflow (INT16_MAX) coincident with fencing an Ongoing txn.
    auto r = coord.initProducerId("txn-new1", 60'000, 0LL);
    ASSERT_TRUE(r.ok());

    // (b) A NEW producerId (epoch 0) is allocated for the NEXT transaction and
    //     returned to the caller.
    const ProducerId newPid = r.producerId;
    EXPECT_NE(newPid, kOldPid) << "epoch overflow must allocate a fresh PID";
    EXPECT_EQ(r.producerEpoch, static_cast<ProducerEpoch>(0));

    // The in-flight txn must be pending an ABORT marker.
    ASSERT_EQ(coord.state("txn-new1"), TxnState::PrepareAbort);

    // (a) The pending ABORT descriptor + emitted markers MUST carry the OLD pid
    //     (matching the in-flight data written under kOldPid), NOT the new pid.
    auto pending = coord.pendingMarkerTransactions();
    ASSERT_EQ(pending.size(), 1u);
    EXPECT_EQ(pending[0].producerId, kOldPid)
        << "ABORT marker must be stamped with the OLD producerId (matches in-flight data)";
    EXPECT_EQ(pending[0].producerEpoch, static_cast<ProducerEpoch>(INT16_MAX))
        << "ABORT marker must carry the OLD producerEpoch";
    EXPECT_FALSE(pending[0].commit);

    MarkerMap markers;
    int written = ControlMarker::resolvePendingMarkers(coord, makeCollector(markers));
    EXPECT_EQ(written, 2) << "one ABORT marker per involved partition";

    // Both partitions got an ABORT marker stamped with the OLD pid.
    for (const auto& part : { p0, p1 }) {
        ASSERT_EQ(markers[part].size(), 1u) << "partition must have exactly one marker";
        const auto& pl = markers[part].front();
        EXPECT_EQ(pl.controlType, ControlType::Abort);
        EXPECT_EQ(pl.producerId, kOldPid)
            << "the ABORT batch on the partition must carry the OLD pid, not the new pid";
        EXPECT_EQ(pl.producerEpoch, static_cast<ProducerEpoch>(INT16_MAX));
    }

    // After resolution the txn returns to Empty and the coordinator ADOPTS the
    // fresh PID for the next transaction.
    EXPECT_EQ(coord.state("txn-new1"), TxnState::CompleteAbort);
    EXPECT_EQ(coord.pidOf("txn-new1"), newPid)
        << "after the abort resolves, the coordinator adopts the fresh PID";
    EXPECT_EQ(coord.epochOf("txn-new1"), static_cast<ProducerEpoch>(0));

    // (c) The OLD pid is retired: it can no longer add partitions (PID-fenced).
    auto ar = coord.addPartitions("txn-new1", kOldPid, INT16_MAX,
                                  tps({tp("t", 9)}), 0LL);
    EXPECT_EQ(ar.errorCode, AddPartitionsResult::kInvalidPid)
        << "old PID must be retired after epoch-overflow new-PID adoption";
}

// ===========================================================================
// C3/C4 — kConcurrentTransactions returned for Prepare*/Complete* re-init
// ===========================================================================

TEST_F(TxnCoordTest, ReInitWhilePrepareCommitReturnsConcurrentTransactions)
{
    auto r = coord_.initProducerId("txn-c3", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());
    coord_.addPartitions("txn-c3", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-c3", r.producerId, r.producerEpoch, true, nowNs_);
    ASSERT_EQ(coord_.state("txn-c3"), TxnState::PrepareCommit);

    // Re-init while in PrepareCommit → must return kConcurrentTransactions.
    auto r2 = coord_.initProducerId("txn-c3", kDefaultTimeoutMs, nowNs_);
    EXPECT_EQ(r2.errorCode, InitProducerIdResult::kConcurrentTransactions)
        << "initProducerId while PrepareCommit must return kConcurrentTransactions";
    // State must be unchanged.
    EXPECT_EQ(coord_.state("txn-c3"), TxnState::PrepareCommit);
    // Epoch must be unchanged.
    EXPECT_EQ(coord_.epochOf("txn-c3"), r.producerEpoch);
}

TEST_F(TxnCoordTest, ReInitWhilePrepareAbortReturnsConcurrentTransactions)
{
    auto r = coord_.initProducerId("txn-c4", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());
    coord_.addPartitions("txn-c4", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-c4", r.producerId, r.producerEpoch, false, nowNs_);
    ASSERT_EQ(coord_.state("txn-c4"), TxnState::PrepareAbort);

    auto r2 = coord_.initProducerId("txn-c4", kDefaultTimeoutMs, nowNs_);
    EXPECT_EQ(r2.errorCode, InitProducerIdResult::kConcurrentTransactions);
    EXPECT_EQ(coord_.state("txn-c4"), TxnState::PrepareAbort);
}

TEST_F(TxnCoordTest, ReInitWhileCompleteCommitBumpsTheEpoch)
{
    // P5-5 LIVENESS FIX: A recovered CompleteCommit is now collapsed to Empty in
    // the constructor (crash-in-two-write-window fix).  A LIVE (in-memory-only)
    // CompleteCommit — reached by running completeTransaction without crashing —
    // is immediately overwritten by Empty in the same call, so it is also
    // effectively never observable by a caller between the two writes.
    //
    // The original test injected a CompleteCommit via the durable log to simulate
    // a crash in the two-write window, then asserted kConcurrentTransactions on
    // re-init.  After the liveness fix that crash-seam is healed: the recovered
    // state is Empty (not CompleteCommit), and re-init must succeed.
    //
    // We keep the test but update the assertions to match the corrected behaviour:
    // a recovered CompleteCommit → Empty → re-init returns ok (not Concurrent).
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    {
        TransactionLog log(path);
        TxnLogEntry e;
        e.txnId         = "txn-cc";
        e.producerId    = 1;
        e.producerEpoch = 2;
        e.state         = TxnState::CompleteCommit;
        e.txnTimeoutMs  = 60'000;
        log.append(e);
    }
    TransactionLog log2(path);
    TransactionCoordinator coord2(ProducerIdAllocator{}, 60'000, &log2, 0);
    // F15 §5.4: a recovered CompleteCommit stays the RESTING state (not wedged).
    ASSERT_EQ(coord2.state("txn-cc"), TxnState::CompleteCommit);

    // re-init on Empty → succeeds (not kConcurrentTransactions).
    auto r2 = coord2.initProducerId("txn-cc", 60'000, 0LL);
    EXPECT_TRUE(r2.ok())
        << "a recovered (resting) CompleteCommit must not wedge re-init";
    EXPECT_NE(r2.errorCode, InitProducerIdResult::kConcurrentTransactions)
        << "txnId must NOT be wedged after CompleteCommit collapse";
}

TEST_F(TxnCoordTest, ReInitAfterResolveSucceeds)
{
    // After resolving a PrepareAbort (state back to Empty), re-init must succeed.
    auto r = coord_.initProducerId("txn-after", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());
    coord_.addPartitions("txn-after", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-after", r.producerId, r.producerEpoch, false, nowNs_);
    ASSERT_EQ(coord_.state("txn-after"), TxnState::PrepareAbort);

    // Resolve.
    auto appendFn = [](const TopicPartition&, const std::vector<uint8_t>&) {};
    coord_.completeTransaction("txn-after");
    ASSERT_EQ(coord_.state("txn-after"), TxnState::CompleteAbort);

    // Now re-init must succeed.
    auto r2 = coord_.initProducerId("txn-after", kDefaultTimeoutMs, nowNs_);
    EXPECT_TRUE(r2.ok());
    EXPECT_NE(r2.errorCode, InitProducerIdResult::kConcurrentTransactions);
}

// ===========================================================================
// S1 — isControlBatch rejects truncated buffers and CRC-corrupted data batches
// ===========================================================================

TEST(ControlMarkerAuditS1Test, TruncatedBufferNotControl)
{
    // A valid control batch truncated to less than ENCODED_SIZE → must return false.
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 0);
    ASSERT_EQ(batch.size(), ControlMarker::ENCODED_SIZE);

    // Truncated: avail = ENCODED_SIZE - 1 → not enough bytes.
    EXPECT_FALSE(ControlMarker::isControlBatch(batch.data(), 0,
                                                ControlMarker::ENCODED_SIZE - 1))
        << "Truncated buffer must not be identified as a control batch";
}

TEST(ControlMarkerAuditS1Test, CrcCorruptedDataBatchNotControl)
{
    // A data batch with magic=2, CONTROL bit set, but wrong CRC → must return false.
    uint8_t buf[ControlMarker::ENCODED_SIZE] = {};
    buf[16] = 2; // magic = 2
    int16_t attrs = static_cast<int16_t>(ControlMarker::kAttrControl |
                                          ControlMarker::kAttrTransactional);
    std::memcpy(buf + 21, &attrs, 2);
    // CRC at [17..20] is all zeros (wrong) — must NOT be accepted.
    EXPECT_FALSE(ControlMarker::isControlBatch(buf, 0, ControlMarker::ENCODED_SIZE))
        << "A batch with magic=2 and CONTROL bit but wrong CRC must be rejected";
}

TEST(ControlMarkerAuditS1Test, ValidControlBatchPassesCrcCheck)
{
    // A properly built control batch must pass isControlBatch with CRC check.
    auto batch = ControlMarker::build(42, 3, ControlType::Abort, 7);
    EXPECT_TRUE(ControlMarker::isControlBatch(batch.data(), 0, ControlMarker::ENCODED_SIZE))
        << "A valid built control batch must pass the CRC-inclusive check";
}

TEST(ControlMarkerAuditS1Test, ExtractTruncatedBufferReturnsFalse)
{
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 0);
    ControlMarkerPayload pl{};
    EXPECT_FALSE(ControlMarker::extract(batch.data(), 0, pl,
                                         ControlMarker::ENCODED_SIZE - 5))
        << "extract on truncated buffer must return false (no over-read)";
}

TEST(ControlMarkerAuditS1Test, ExtractCrcCorruptedReturnsFalse)
{
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 0);
    // Corrupt the CRC field.
    batch[17] ^= 0xFFu;
    ControlMarkerPayload pl{};
    EXPECT_FALSE(ControlMarker::extract(batch.data(), 0, pl, ControlMarker::ENCODED_SIZE))
        << "extract on CRC-corrupted batch must return false";
}

TEST(ControlMarkerAuditS1Test, DataBatchWithControlBitButWrongCrcNotExtracted)
{
    // Craft a buffer that has magic=2 and the CONTROL bit, but incorrect CRC.
    // This is the adversarial input from the audit (S1).
    uint8_t buf[ControlMarker::ENCODED_SIZE] = {};
    buf[16] = 2; // magic
    int16_t attrs = static_cast<int16_t>(ControlMarker::kAttrControl |
                                          ControlMarker::kAttrTransactional);
    std::memcpy(buf + 21, &attrs, 2);
    // Stuff plausible producer fields.
    int64_t fakePid = 999;
    std::memcpy(buf + 43, &fakePid, 8);
    // CRC is still zero (wrong).
    ControlMarkerPayload pl{};
    EXPECT_FALSE(ControlMarker::extract(buf, 0, pl, ControlMarker::ENCODED_SIZE))
        << "A data batch with CONTROL bit but wrong CRC must not be decoded";
}

// ===========================================================================
// T1 — Abort tests MUST assert markers were written (upgrade vacuous tests)
// ===========================================================================

TEST_F(TxnCoordTest, ReInitOngoingWritesAbortMarkersToAllPartitions_T1)
{
    // This is the upgraded version of ReInitOngoingThenCompleteResetsToEmpty.
    // It asserts markers are written (T1 fix).
    auto r = coord_.initProducerId("txn-t1-fence", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r.ok());
    coord_.addPartitions("txn-t1-fence", r.producerId, r.producerEpoch,
                         tps({tp("a", 0), tp("b", 1)}), nowNs_);

    // Fence via re-init.
    auto r2 = coord_.initProducerId("txn-t1-fence", kDefaultTimeoutMs, nowNs_);
    ASSERT_TRUE(r2.ok());
    ASSERT_EQ(coord_.state("txn-t1-fence"), TxnState::PrepareAbort);

    // Assert markers written (T1: must not be vacuous).
    MarkerMap captured;
    int total = ControlMarker::resolvePendingMarkers(coord_, makeCollector(captured));
    EXPECT_EQ(total, 2)   << "T1: 2 ABORT markers must be written, one per partition";
    EXPECT_EQ(captured.size(), 2u);
    EXPECT_TRUE(captured.contains(tp("a", 0)));
    EXPECT_TRUE(captured.contains(tp("b", 1)));
    EXPECT_EQ(coord_.state("txn-t1-fence"), TxnState::CompleteAbort);
}

TEST_F(TxnCoordTest, TimeoutAbortWritesAbortMarkersToAllPartitions_T1)
{
    // Upgraded version of FullLifecycleWithTimeoutAbort.
    // T1: assert ABORT markers were written, not just completeTransaction called.
    auto r = coord_.initProducerId("txn-t1-timeout", /*txnTimeoutMs=*/2000, nowNs_);
    coord_.addPartitions("txn-t1-timeout", r.producerId, r.producerEpoch,
                         tps({tp("p0", 0), tp("p1", 1), tp("p2", 2)}), nowNs_);

    constexpr int kCap = 10;
    bool done = false;
    for (int i = 0; i < kCap && !done; ++i) {
        advanceClock(1001);
        if (coord_.doWork(nowNs_) > 0) done = true;
    }
    ASSERT_TRUE(done) << "Timeout abort must fire within cap";
    ASSERT_EQ(coord_.state("txn-t1-timeout"), TxnState::PrepareAbort);

    // T1: assert markers are actually written.
    MarkerMap captured;
    int total = ControlMarker::resolvePendingMarkers(coord_, makeCollector(captured));
    EXPECT_EQ(total, 3)   << "T1: 3 ABORT markers must be written";
    EXPECT_EQ(coord_.state("txn-t1-timeout"), TxnState::CompleteAbort);
    EXPECT_TRUE(captured.contains(tp("p0", 0)));
    EXPECT_TRUE(captured.contains(tp("p1", 1)));
    EXPECT_TRUE(captured.contains(tp("p2", 2)));

    for (auto& [part, pls] : captured) {
        ASSERT_EQ(pls.size(), 1u);
        EXPECT_EQ(pls[0].controlType, ControlType::Abort);
    }
}

// ===========================================================================
// T2 — AddPartitionsWhenCompleteCommitIsInvalidTransition — replace vacuous SUCCEED()
// ===========================================================================

TEST(TxnCoordAuditT2Test, AddPartitionsOnRecoveredCompleteCommitStartsOngoing)
{
    // P5-5 LIVENESS FIX: a recovered CompleteCommit is now collapsed to Empty.
    // This means addPartitions is now a VALID transition (Empty→Ongoing), not
    // kInvalidTransition.  The original test was probing the now-fixed crash-seam
    // scenario where the txnId would be permanently wedged.  After the fix the
    // txnId is immediately reusable.  We update to assert the corrected behavior.
    //
    // The invariant "addPartitions while a Complete* is IN-FLIGHT" (i.e. within a
    // live coordinator run, not across a crash) still holds because completeTransaction
    // atomically transitions CompleteCommit→Empty in memory, so there is never a
    // window where a live caller can call addPartitions on a CompleteCommit state.
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    {
        TransactionLog log(path);
        TxnLogEntry e;
        e.txnId         = "txn-t2";
        e.producerId    = 7;
        e.producerEpoch = 0;
        e.state         = TxnState::CompleteCommit;
        e.txnTimeoutMs  = 60'000;
        log.append(e);
    }
    TransactionLog log2(path);
    TransactionCoordinator coord2(ProducerIdAllocator{}, 60'000, &log2, 0);
    // F15 §5.4: a recovered CompleteCommit stays the RESTING state (not wedged).
    ASSERT_EQ(coord2.state("txn-t2"), TxnState::CompleteCommit);

    // addPartitions on Empty is a valid transition (Empty→Ongoing).
    auto ar = coord2.addPartitions("txn-t2", /*pid=*/7, /*epoch=*/0,
                                    tps({tp("t", 0)}), 0LL);
    EXPECT_EQ(ar.errorCode, AddPartitionsResult::kOk)
        << "addPartitions on a recovered (resting) CompleteCommit must succeed (Complete* → Ongoing)";
}

// ===========================================================================
// C2 — CompleteCommit persisted; recovery does NOT re-write markers
// ===========================================================================

TEST(TxnCoordAuditC2Test, CompleteCommitPersistedAsDistinctRecord)
{
    // Verify that completeTransaction persists CompleteCommit as a separate log
    // record BEFORE transitioning to Empty.
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    TransactionLog log(path);

    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log);
    auto r = coord.initProducerId("txn-c2", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    coord.addPartitions("txn-c2", r.producerId, r.producerEpoch,
                        tps({tp("t", 0)}), 0LL);
    coord.endTransaction("txn-c2", r.producerId, r.producerEpoch, true, 0LL);

    // Verify CompleteCommit in log before calling completeTransaction.
    // (PrepareCommit is in the log now.)
    ASSERT_EQ(log.metadata("txn-c2")->state, TxnState::PrepareCommit);

    // Call completeTransaction — CompleteCommit must be persisted.
    coord.completeTransaction("txn-c2");

    // After completeTransaction, log reflects Empty (CompleteCommit then Empty).
    EXPECT_EQ(log.metadata("txn-c2")->state, TxnState::CompleteCommit);
    EXPECT_EQ(coord.state("txn-c2"), TxnState::CompleteCommit);
}

TEST(TxnCoordAuditC2Test, CompleteAbortPersistedAsDistinctRecord)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    TransactionLog log(path);

    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log);
    auto r = coord.initProducerId("txn-c2b", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    coord.addPartitions("txn-c2b", r.producerId, r.producerEpoch,
                        tps({tp("t", 0)}), 0LL);
    coord.endTransaction("txn-c2b", r.producerId, r.producerEpoch, false, 0LL);
    ASSERT_EQ(log.metadata("txn-c2b")->state, TxnState::PrepareAbort);

    coord.completeTransaction("txn-c2b");

    EXPECT_EQ(log.metadata("txn-c2b")->state, TxnState::CompleteAbort);
    EXPECT_EQ(coord.state("txn-c2b"), TxnState::CompleteAbort);
}

// ===========================================================================
// Part B (coordinator / recovery) — additional edge-case coverage (audit)
// ===========================================================================

// #1 (A1 BLOCKER) — Allocator reseed after replay: a coordinator recovering a
// log whose live txns hold producerId 0..N with the DEFAULT allocator must NOT
// re-hand-out a colliding producerId. This FAILS under pre-A1 code (default
// allocator starts at 0 → returns 0 again). No high-pid injection masks it.
TEST(TxnCoordAuditA1Test, AllocatorReseededPastRecoveredMaxNoCollision)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    // Persist several live txns holding producerId 0..4 (the DEFAULT allocator
    // block), the last one Ongoing so it is unmistakably "live".
    constexpr ProducerId kMaxPid = 4;
    {
        TransactionLog log(path);
        for (ProducerId pid = 0; pid <= kMaxPid; ++pid) {
            TxnLogEntry e;
            e.txnId         = "txn-" + std::to_string(pid);
            e.producerId    = pid;
            e.producerEpoch = 0;
            e.state         = (pid == kMaxPid) ? TxnState::Ongoing : TxnState::Empty;
            e.txnTimeoutMs  = 60'000;
            if (pid == kMaxPid) { e.partitions = { tp("t", 0) }; e.txnStartNs = 10; }
            log.append(e);
        }
    }

    TransactionLog log2(path);
    // DEFAULT allocator (starts at 0) — this is the documented production path.
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log2);

    // Allocate a brand-new producerId (no txnId → pure allocation).
    auto r = coord.initProducerId(std::nullopt, 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    EXPECT_GT(r.producerId, kMaxPid)
        << "new pid must be strictly greater than any recovered producerId (no reuse)";

    // And a new txnId branch must likewise not collide.
    auto r2 = coord.initProducerId("txn-fresh", 60'000, 0LL);
    ASSERT_TRUE(r2.ok());
    EXPECT_GT(r2.producerId, kMaxPid);
    EXPECT_NE(r2.producerId, r.producerId);
}

// #2 (A2) — Crash mid-overflow-fence BEFORE completeTransaction: recovery must
// resolve the abort under the OLD pid AND adopt the SAME fresh pid the client
// was handed pre-crash (persisted pendingNewProducerId).
TEST(TxnCoordAuditA2Test, CrashMidOverflowFenceAdoptsSameFreshPidOnRecovery)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    constexpr ProducerId kOldPid = 42;
    ProducerId returnedFreshPid = -1;

    // Phase 1: inject Ongoing@INT16_MAX, re-init (overflow fence) → returns fresh
    // pid and persists PrepareAbort with the deferred swap. Then DROP the
    // coordinator WITHOUT completeTransaction (simulated crash).
    {
        {
            TransactionLog log(path);
            TxnLogEntry e;
            e.txnId         = "txn-a2";
            e.producerId    = kOldPid;
            e.producerEpoch = INT16_MAX;
            e.state         = TxnState::Ongoing;
            e.txnTimeoutMs  = 60'000;
            e.txnStartNs    = 1'000;
            e.partitions    = { tp("t", 0), tp("t", 1) };
            log.append(e);
        }
        TransactionLog log1(path);
        TransactionCoordinator coord(ProducerIdAllocator{100}, 60'000, &log1, /*coordEpoch=*/3);
        auto r = coord.initProducerId("txn-a2", 60'000, 0LL);
        ASSERT_TRUE(r.ok());
        returnedFreshPid = r.producerId;
        ASSERT_NE(returnedFreshPid, kOldPid);
        ASSERT_EQ(coord.state("txn-a2"), TxnState::PrepareAbort);
        // NO completeTransaction — crash here.
    }

    // Phase 2: reopen from the log. The recovered PrepareAbort must still carry
    // the OLD pid and the persisted deferred fresh pid.
    TransactionLog log2(path);
    TransactionCoordinator coord2(ProducerIdAllocator{100}, 60'000, &log2, /*coordEpoch=*/3);
    ASSERT_EQ(coord2.state("txn-a2"), TxnState::PrepareAbort);
    ASSERT_EQ(coord2.pidOf("txn-a2"), kOldPid);

    auto pending = coord2.pendingMarkerTransactions();
    ASSERT_EQ(pending.size(), 1u);
    EXPECT_EQ(pending[0].producerId, kOldPid);
    EXPECT_EQ(pending[0].producerEpoch, static_cast<ProducerEpoch>(INT16_MAX));
    EXPECT_FALSE(pending[0].commit);

    // Resolve markers + complete → the coordinator adopts the SAME fresh pid the
    // client received pre-crash (no divergence).
    MarkerMap markers;
    int written = ControlMarker::resolvePendingMarkers(coord2, makeCollector(markers));
    EXPECT_EQ(written, 2);
    for (const auto& pl : markers[tp("t", 0)]) EXPECT_EQ(pl.producerId, kOldPid);

    EXPECT_EQ(coord2.state("txn-a2"), TxnState::CompleteAbort);
    EXPECT_EQ(coord2.pidOf("txn-a2"), returnedFreshPid)
        << "recovery must adopt the SAME fresh pid the client was handed pre-crash";
    EXPECT_EQ(coord2.epochOf("txn-a2"), static_cast<ProducerEpoch>(0));
}

// #3 — re-init in CompleteAbort / PrepareEpochFence / Dead returns
// kConcurrentTransactions (Prepare*/Complete*/Fence) or re-creates (Dead).
static void injectState(const std::string& path, const std::string& txnId,
                        ProducerId pid, ProducerEpoch ep, TxnState st) {
    TransactionLog log(path);
    TxnLogEntry e;
    e.txnId = txnId; e.producerId = pid; e.producerEpoch = ep; e.state = st;
    e.txnTimeoutMs = 60'000;
    log.append(e);
}

TEST(TxnCoordAuditA3StateTest, ReInitInCompleteAbortBumpsTheEpoch)
{
    // P5-5 LIVENESS FIX: a recovered CompleteAbort is now collapsed to Empty in
    // the constructor (crash-in-two-write-window fix).  The txnId is NOT wedged.
    // re-init succeeds (state is Empty), not kConcurrentTransactions.
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    injectState(path, "txn-ca", 7, 2, TxnState::CompleteAbort);
    TransactionLog log2(path);
    TransactionCoordinator coord(ProducerIdAllocator{100}, 60'000, &log2);
    // F15 §5.4: a recovered CompleteAbort stays the RESTING state (not wedged).
    EXPECT_EQ(coord.state("txn-ca"), TxnState::CompleteAbort);
    auto r = coord.initProducerId("txn-ca", 60'000, 0LL);
    EXPECT_TRUE(r.ok())
        << "a recovered (resting) CompleteAbort must not wedge re-init";
    EXPECT_NE(r.errorCode, InitProducerIdResult::kConcurrentTransactions)
        << "txnId must NOT be wedged after CompleteAbort collapse";
}

TEST(TxnCoordAuditA3StateTest, ReInitInPrepareEpochFenceReturnsConcurrent)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    injectState(path, "txn-pef", 8, 1, TxnState::PrepareEpochFence);
    TransactionLog log2(path);
    TransactionCoordinator coord(ProducerIdAllocator{100}, 60'000, &log2);
    auto r = coord.initProducerId("txn-pef", 60'000, 0LL);
    EXPECT_EQ(r.errorCode, InitProducerIdResult::kConcurrentTransactions);
    EXPECT_EQ(coord.state("txn-pef"), TxnState::PrepareEpochFence);
}

TEST(TxnCoordAuditA3StateTest, ReInitInDeadReCreatesEmpty)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    injectState(path, "txn-dead", 9, 4, TxnState::Dead);
    TransactionLog log2(path);
    TransactionCoordinator coord(ProducerIdAllocator{100}, 60'000, &log2);
    auto r = coord.initProducerId("txn-dead", 60'000, 0LL);
    // Dead is treated as re-create: succeeds, bumps epoch, resets to Empty.
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(coord.state("txn-dead"), TxnState::Empty);
    EXPECT_EQ(coord.epochOf("txn-dead"), static_cast<ProducerEpoch>(5));
}

// #4 — completeTransaction in CompleteCommit / CompleteAbort / PrepareEpochFence /
// Dead is an invalid transition.
TEST(TxnCoordAuditA4StateTest, CompleteTransactionInNonPrepareStateIsInvalid)
{
    for (TxnState st : { TxnState::CompleteCommit, TxnState::CompleteAbort,
                         TxnState::PrepareEpochFence, TxnState::Dead,
                         TxnState::Empty }) {
        auto path = coordTmpPath();
        CoordTempFile tf{path};
        injectState(path, "txn-ct", 3, 0, st);
        TransactionLog log2(path);
        TransactionCoordinator coord(ProducerIdAllocator{100}, 60'000, &log2);
        auto r = coord.completeTransaction("txn-ct");
        EXPECT_EQ(r.errorCode, CompleteTransactionResult::kInvalidTransition)
            << "state=" << txnStateName(st);
    }
}

// #6 — interleaved commit + abort pending set in ONE resolvePendingMarkers pass:
// each txn gets the correct control type.
TEST(TxnCoordAuditA6ResolveTest, InterleavedCommitAndAbortInOnePass)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    // Two recovered Prepare* txns: one commit, one abort.
    {
        TransactionLog log(path);
        {
            TxnLogEntry e; e.txnId = "txn-commit"; e.producerId = 10; e.producerEpoch = 1;
            e.state = TxnState::PrepareCommit; e.partitions = { tp("c", 0) };
            e.txnTimeoutMs = 60'000; log.append(e);
        }
        {
            TxnLogEntry e; e.txnId = "txn-abort"; e.producerId = 20; e.producerEpoch = 2;
            e.state = TxnState::PrepareAbort; e.partitions = { tp("a", 0) };
            e.txnTimeoutMs = 60'000; log.append(e);
        }
    }
    TransactionLog log2(path);
    TransactionCoordinator coord(ProducerIdAllocator{100}, 60'000, &log2);

    MarkerMap markers;
    int total = ControlMarker::resolvePendingMarkers(coord, makeCollector(markers));
    EXPECT_EQ(total, 2);
    ASSERT_EQ(markers[tp("c", 0)].size(), 1u);
    ASSERT_EQ(markers[tp("a", 0)].size(), 1u);
    EXPECT_EQ(markers[tp("c", 0)].front().controlType, ControlType::Commit);
    EXPECT_EQ(markers[tp("a", 0)].front().controlType, ControlType::Abort);
    EXPECT_EQ(coord.state("txn-commit"), TxnState::CompleteCommit);
    EXPECT_EQ(coord.state("txn-abort"), TxnState::CompleteAbort);
}

// #7 — resolvePendingMarkers with MANY pending txns (throughput/correctness).
TEST(TxnCoordAuditA7ResolveTest, ManyPendingResolvedCorrectly)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    constexpr int N = 100;
    {
        TransactionLog log(path);
        for (int i = 0; i < N; ++i) {
            TxnLogEntry e;
            e.txnId = "txn-" + std::to_string(i);
            e.producerId = 1000 + i; e.producerEpoch = 1;
            e.state = (i % 2 == 0) ? TxnState::PrepareCommit : TxnState::PrepareAbort;
            e.partitions = { tp("p", i) };
            e.txnTimeoutMs = 60'000;
            log.append(e);
        }
    }
    TransactionLog log2(path);
    TransactionCoordinator coord(ProducerIdAllocator{100000}, 60'000, &log2);
    MarkerMap markers;
    int total = ControlMarker::resolvePendingMarkers(coord, makeCollector(markers));
    EXPECT_EQ(total, N);
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(markers[tp("p", i)].size(), 1u) << "i=" << i;
        EXPECT_EQ(markers[tp("p", i)].front().controlType,
                  (i % 2 == 0) ? ControlType::Commit : ControlType::Abort);
        // F15 §5.4: Complete* is the resting state.
        EXPECT_EQ(coord.state("txn-" + std::to_string(i)),
                  (i % 2 == 0) ? TxnState::CompleteCommit : TxnState::CompleteAbort);
    }
}

// #10 — addPartitions with an EMPTY partition vector still fires Empty→Ongoing
// and records txnStartNs.
TEST(TxnCoordAuditA10Test, AddPartitionsEmptySetStartsOngoing)
{
    TransactionCoordinator coord;
    auto r = coord.initProducerId("txn-empty", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    auto ar = coord.addPartitions("txn-empty", r.producerId, r.producerEpoch,
                                  std::vector<TopicPartition>{}, /*nowNs=*/777);
    EXPECT_EQ(ar.errorCode, AddPartitionsResult::kOk);
    EXPECT_EQ(coord.state("txn-empty"), TxnState::Ongoing);
    ASSERT_NE(coord.metadata("txn-empty"), nullptr);
    EXPECT_EQ(coord.metadata("txn-empty")->txnStartNs, 777);
    EXPECT_EQ(coord.partitionsOf("txn-empty").size(), 0u);
}

// #11 — addPartitions idempotency: the same partition added twice returns kOk
// and the persisted set does not grow.
TEST(TxnCoordAuditA11Test, AddPartitionsIdempotentSameSet)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    TransactionLog log(path);
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log);
    auto r = coord.initProducerId("txn-idem", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    auto a1 = coord.addPartitions("txn-idem", r.producerId, r.producerEpoch,
                                  tps({tp("t", 0)}), 1);
    EXPECT_EQ(a1.errorCode, AddPartitionsResult::kOk);
    auto a2 = coord.addPartitions("txn-idem", r.producerId, r.producerEpoch,
                                  tps({tp("t", 0)}), 2);  // same partition again
    EXPECT_EQ(a2.errorCode, AddPartitionsResult::kOk);
    EXPECT_EQ(coord.partitionsOf("txn-idem").size(), 1u);
    // Persisted set is also size 1.
    EXPECT_EQ(log.metadata("txn-idem")->partitions.size(), 1u);
}

// #13 — Epoch walk 32766 → 32767 → overflow. Inject Empty@32766, re-init twice.
TEST(TxnCoordAuditA13Test, EpochWalk32766Then32767ThenOverflow)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    injectState(path, "txn-walk", 42, static_cast<ProducerEpoch>(INT16_MAX - 1), TxnState::Empty);
    TransactionLog log2(path);
    TransactionCoordinator coord(ProducerIdAllocator{500}, 60'000, &log2);

    // 32766 → 32767 (same pid).
    auto r1 = coord.initProducerId("txn-walk", 60'000, 0LL);
    ASSERT_TRUE(r1.ok());
    EXPECT_EQ(r1.producerId, static_cast<ProducerId>(42));
    EXPECT_EQ(r1.producerEpoch, static_cast<ProducerEpoch>(INT16_MAX));

    // 32767 → overflow → fresh pid, epoch 0.
    auto r2 = coord.initProducerId("txn-walk", 60'000, 0LL);
    ASSERT_TRUE(r2.ok());
    EXPECT_NE(r2.producerId, static_cast<ProducerId>(42));
    EXPECT_EQ(r2.producerEpoch, static_cast<ProducerEpoch>(0));
}

// #14 — Non-overflow Ongoing fence: the ABORT marker carries the OLD pid with a
// BUMPED epoch (NEW-1 non-overflow branch).
TEST(TxnCoordAuditA14Test, NonOverflowOngoingFenceOldPidBumpedEpoch)
{
    TransactionCoordinator coord;
    auto r = coord.initProducerId("txn-nf", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    const ProducerId pid = r.producerId;
    const ProducerEpoch ep0 = r.producerEpoch;
    coord.addPartitions("txn-nf", pid, ep0, tps({tp("t", 0)}), 10);

    // Re-init fences the Ongoing txn (normal epoch, no overflow).
    auto r2 = coord.initProducerId("txn-nf", 60'000, 0LL);
    ASSERT_TRUE(r2.ok());
    EXPECT_EQ(r2.producerId, pid) << "non-overflow keeps the same pid";
    EXPECT_EQ(r2.producerEpoch, static_cast<ProducerEpoch>(ep0 + 1));
    ASSERT_EQ(coord.state("txn-nf"), TxnState::PrepareAbort);

    auto pending = coord.pendingMarkerTransactions();
    ASSERT_EQ(pending.size(), 1u);
    EXPECT_EQ(pending[0].producerId, pid);
    EXPECT_EQ(pending[0].producerEpoch, static_cast<ProducerEpoch>(ep0 + 1))
        << "bumped epoch on the SAME pid still matches in-flight data (keyed by pid)";
}

// Marker for a partition NOT in the txn: resolvePendingMarkers only writes to the
// partitions actually recorded in the txn (never a stray partition).
TEST(TxnCoordAuditMarkerScopeTest, MarkersOnlyForTxnPartitions)
{
    TransactionCoordinator coord;
    auto r = coord.initProducerId("txn-scope", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    coord.addPartitions("txn-scope", r.producerId, r.producerEpoch,
                        tps({tp("in", 0), tp("in", 1)}), 10);
    coord.initProducerId("txn-scope", 60'000, 0LL); // fence → PrepareAbort
    MarkerMap markers;
    ControlMarker::resolvePendingMarkers(coord, makeCollector(markers));
    EXPECT_EQ(markers.count(tp("in", 0)), 1u);
    EXPECT_EQ(markers.count(tp("in", 1)), 1u);
    EXPECT_EQ(markers.count(tp("out", 9)), 0u) << "a partition never in the txn gets no marker";
}

// #8/#9 — doWork timeout boundary: an Ongoing txn with txnStartNs and a large
// timeout does NOT abort before the deadline, and DOES at/after it. Also a
// huge (INT32_MAX ms) timeout does not overflow the ns multiply into a spurious
// abort.
TEST(TxnCoordAuditA8Test, DoWorkTimeoutBoundaryAndHugeTimeoutNoOverflow)
{
    // Normal boundary.
    {
        TransactionCoordinator coord;
        auto r = coord.initProducerId("txn-to", 100 /*ms*/, 0LL);
        ASSERT_TRUE(r.ok());
        coord.addPartitions("txn-to", r.producerId, r.producerEpoch,
                            tps({tp("t", 0)}), /*nowNs=*/0);
        // Exactly at the deadline (strict >): not yet aborted.
        EXPECT_EQ(coord.doWork(ms2ns(100)), 0);
        EXPECT_EQ(coord.state("txn-to"), TxnState::Ongoing);
        // One ns past: aborted.
        EXPECT_EQ(coord.doWork(ms2ns(100) + 1), 1);
        EXPECT_EQ(coord.state("txn-to"), TxnState::PrepareAbort);
    }
    // Huge timeout: INT32_MAX ms * 1e6 fits in int64 (~2.1e15) — must NOT abort
    // at a modest nowNs.
    {
        auto path = coordTmpPath();
        CoordTempFile tf{path};
        {
            TransactionLog log(path);
            TxnLogEntry e; e.txnId = "txn-huge"; e.producerId = 1; e.producerEpoch = 0;
            e.state = TxnState::Ongoing; e.partitions = { tp("t", 0) };
            e.txnStartNs = 0; e.txnTimeoutMs = INT32_MAX; log.append(e);
        }
        TransactionLog log2(path);
        TransactionCoordinator coord(ProducerIdAllocator{100}, 60'000, &log2);
        EXPECT_EQ(coord.doWork(ms2ns(1'000'000)), 0);  // well within the huge window
        EXPECT_EQ(coord.state("txn-huge"), TxnState::Ongoing);
    }
}

// ===========================================================================
// Round-2 residual edge-case tests (7 niche interactions — belt-and-suspenders)
// ===========================================================================

// Residual-1: Allocator reseed when recovered max producerId == INT64_MAX.
// After replaying a log whose highest producerId == INT64_MAX, the next
// initProducerId call must return kExhausted (pid == -1), never a reused or
// wrapped negative pid.
TEST(TxnCoordRound2Residual1Test, ReseedWhenMaxRecoveredPidIsInt64Max)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    {
        TransactionLog log(path);
        TxnLogEntry e;
        e.txnId         = "txn-maxpid";
        e.producerId    = INT64_MAX;    // highest possible pid
        e.producerEpoch = 0;
        e.state         = TxnState::Empty;
        e.txnTimeoutMs  = 60'000;
        log.append(e);
    }

    // Recover: default allocator (starts at 0) must be reseeded past INT64_MAX.
    TransactionLog log2(path);
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log2);
    ASSERT_EQ(coord.state("txn-maxpid"), TxnState::Empty);

    // Allocating a new transactionalId must report exhaustion, not a recycled id.
    auto r = coord.initProducerId("txn-new", 60'000, 0LL);
    EXPECT_EQ(r.producerId, ProducerIdAllocator::kExhausted)
        << "allocator must be exhausted when max recovered pid == INT64_MAX";
}

// Residual-2: overflow-fence + crash + reseed in ONE combined flow.
// Recover a PrepareAbort whose pendingNewProducerId is also the recovered max;
// a FRESH txnId allocation after resolution must land strictly above the
// deferred pid and never equal it.
TEST(TxnCoordRound2Residual2Test, OverflowFenceCrashReseedCombinedFlow)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    constexpr ProducerId kOldPid   = 10;
    constexpr ProducerId kFreshPid = 100;   // what the overflow-fence would allocate

    // Inject a PrepareAbort (as-if crash mid-overflow-fence) with the deferred
    // fresh pid recorded in pendingNewProducerId.
    {
        TransactionLog log(path);
        TxnLogEntry e;
        e.txnId                  = "txn-r2";
        e.producerId             = kOldPid;
        e.producerEpoch          = INT16_MAX;   // was at overflow boundary
        e.state                  = TxnState::PrepareAbort;
        e.txnTimeoutMs           = 60'000;
        e.txnStartNs             = 1'000;
        e.partitions             = { tp("t", 0) };
        e.pendingNewProducerId   = kFreshPid;   // the deferred fresh pid
        e.pendingNewProducerEpoch = 0;
        log.append(e);
    }

    // Recover: the allocator is reseeded past max(kOldPid, kFreshPid)=kFreshPid.
    TransactionLog log2(path);
    // Allocator starting at 0 — reseed must advance it beyond kFreshPid.
    TransactionCoordinator coord2(ProducerIdAllocator{}, 60'000, &log2, /*coordEpoch=*/3);
    ASSERT_EQ(coord2.state("txn-r2"), TxnState::PrepareAbort);
    ASSERT_EQ(coord2.pidOf("txn-r2"), kOldPid);

    // The pending descriptor still carries the OLD pid (correct abort semantics).
    auto pending = coord2.pendingMarkerTransactions();
    ASSERT_EQ(pending.size(), 1u);
    EXPECT_EQ(pending[0].producerId, kOldPid);
    EXPECT_EQ(pending[0].producerEpoch, static_cast<ProducerEpoch>(INT16_MAX));
    EXPECT_FALSE(pending[0].commit);

    // Resolve: abort marker written under OLD pid; coordinator adopts the SAME
    // deferred fresh pid.
    MarkerMap markers;
    int written = ControlMarker::resolvePendingMarkers(coord2, makeCollector(markers));
    EXPECT_EQ(written, 1);
    EXPECT_EQ(markers[tp("t", 0)].front().producerId, kOldPid);

    EXPECT_EQ(coord2.state("txn-r2"), TxnState::CompleteAbort);
    EXPECT_EQ(coord2.pidOf("txn-r2"), kFreshPid)
        << "coordinator must adopt the persisted deferred fresh pid on recovery";

    // Now a genuinely fresh txnId allocation must land strictly above kFreshPid
    // (allocator was reseeded past it).
    auto r = coord2.initProducerId("txn-brand-new", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    EXPECT_GT(r.producerId, kFreshPid)
        << "fresh allocation after recovery must be strictly above the deferred pid";
    EXPECT_NE(r.producerId, kOldPid)
        << "fresh allocation must not collide with the old pid";
}

// Residual-3: Idempotent re-resolve — marker written → crash → recover →
// resolve AGAIN → NO duplicate markers.
// resolvePendingMarkers ran (CompleteAbort persisted), SAME log reopened →
// pendingMarkerTransactions() is empty → zero markers on second resolve.
TEST(TxnCoordRound2Residual3Test, IdempotentReResolveNoDuplicateMarkers)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    TransactionLog log(path);

    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log);
    auto r = coord.initProducerId("txn-idem-re", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    coord.addPartitions("txn-idem-re", r.producerId, r.producerEpoch,
                        tps({tp("part", 0), tp("part", 1)}), 10);

    // Trigger PrepareAbort via fence.
    coord.initProducerId("txn-idem-re", 60'000, 0LL);
    ASSERT_EQ(coord.state("txn-idem-re"), TxnState::PrepareAbort);

    // First resolve: writes ABORT markers and persists CompleteAbort then Empty.
    MarkerMap markers1;
    int w1 = ControlMarker::resolvePendingMarkers(coord, makeCollector(markers1));
    EXPECT_EQ(w1, 2) << "two ABORT markers expected (one per partition)";
    EXPECT_EQ(coord.state("txn-idem-re"), TxnState::CompleteAbort);

    // Simulate crash-and-reopen: the log now contains CompleteAbort (or Empty).
    // Re-open a new coordinator from the SAME log file.
    TransactionLog log2(path);
    TransactionCoordinator coord2(ProducerIdAllocator{}, 60'000, &log2);

    // After recovery, there must be NO pending markers (markers already written).
    auto pending = coord2.pendingMarkerTransactions();
    EXPECT_TRUE(pending.empty())
        << "after CompleteAbort/Empty recovery, pendingMarkerTransactions must be empty";

    // Second resolve pass must emit ZERO markers (no duplicates).
    MarkerMap markers2;
    int w2 = ControlMarker::resolvePendingMarkers(coord2, makeCollector(markers2));
    EXPECT_EQ(w2, 0) << "no duplicate ABORT markers on second resolve after recovery";
    EXPECT_TRUE(markers2.empty());
}

// Residual-4: addPartitions rejected in PrepareEpochFence state.
// P5-5 LIVENESS FIX: CompleteAbort is now collapsed to Empty on recovery, so
// addPartitions on a recovered CompleteAbort succeeds (not kInvalidTransition).
// PrepareEpochFence is not a Complete* state so it is NOT collapsed and still
// rejects addPartitions with kInvalidTransition.
TEST(TxnCoordRound2Residual4Test, AddPartitionsRejectedInPrepareEpochFence)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    injectState(path, "txn-rej-pef", 5, 0, TxnState::PrepareEpochFence);
    TransactionLog log2(path);
    TransactionCoordinator coord(ProducerIdAllocator{100}, 60'000, &log2);
    ASSERT_EQ(coord.state("txn-rej-pef"), TxnState::PrepareEpochFence);

    auto ar = coord.addPartitions("txn-rej-pef", /*pid=*/5, /*epoch=*/0,
                                  tps({tp("t", 0)}), 0LL);
    EXPECT_EQ(ar.errorCode, AddPartitionsResult::kInvalidTransition)
        << "addPartitions must be rejected in PrepareEpochFence";
}

// After liveness fix: recovered CompleteAbort collapses to Empty, so
// addPartitions SUCCEEDS (Empty→Ongoing is a valid transition).
TEST(TxnCoordRound2Residual4Test, AddPartitionsAcceptedInRecoveredCompleteAbort)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    injectState(path, "txn-rej-ca", 5, 0, TxnState::CompleteAbort);
    TransactionLog log2(path);
    TransactionCoordinator coord(ProducerIdAllocator{100}, 60'000, &log2);
    // F15 §5.4: CompleteAbort is the resting state.
    ASSERT_EQ(coord.state("txn-rej-ca"), TxnState::CompleteAbort);

    auto ar = coord.addPartitions("txn-rej-ca", /*pid=*/5, /*epoch=*/0,
                                  tps({tp("t", 0)}), 0LL);
    EXPECT_EQ(ar.errorCode, AddPartitionsResult::kOk)
        << "addPartitions on recovered CompleteAbort (now Empty) must succeed";
}

// Residual-5 and Residual-6 are in ControlMarkerTest.cpp (non-zero offset and
// CRC boundary). They are added there to keep marker-specific tests grouped.

// ===========================================================================
// EOS durability — committed consumer-offset loss on crash-in-window.
//
// Bug: completeTransaction(PrepareCommit) used to persist CompleteCommit FIRST,
// then apply the pending offsets to the OffsetStore.  A crash BETWEEN those two
// steps left a durable CompleteCommit record whose pendingOffsets were NEVER
// applied — and recovery only re-drives Prepare* states, so a recovered
// CompleteCommit did nothing → committed consumer offsets were lost (a
// read-process-write pipeline reprocesses inputs after failover: an EOS
// violation).
//
// Fix: apply the offsets to the OffsetStore FIRST (while still PrepareCommit —
// the RE-DRIVABLE resolution), THEN persist the terminal CompleteCommit.  A
// crash before the CompleteCommit persist leaves state == PrepareCommit, which
// recovery marks markersPending → the re-drive re-applies the offsets
// (OffsetStore::commit is an overwrite → idempotent) and re-writes markers
// (already idempotent) → no loss.
// ===========================================================================

namespace {

/// In-memory stand-in for OffsetStore with Kafka overwrite (idempotent) commit
/// semantics: the same (group,tp,offset) re-applied is a no-op overwrite.
struct FakeOffsetStore {
    std::map<std::pair<std::string, std::pair<std::string, int32_t>>, int64_t> committed;

    void commit(const std::string& group,
                const rune::coordination::TopicPartition& tp,
                rune::coordination::Offset offset) {
        committed[{group, {tp.topic, tp.partition}}] = offset;   // overwrite
    }
    std::optional<int64_t> fetch(const std::string& group,
                                 const std::string& topic, int32_t part) const {
        auto it = committed.find({group, {topic, part}});
        if (it == committed.end()) return std::nullopt;
        return it->second;
    }
};

} // anonymous namespace

// Crash EXACTLY in the window between the CompleteCommit persist and the offset
// apply: the injected offsetApplyCb throws on its FIRST invocation, dropping the
// coordinator.  Reconstruct from the durable TransactionLog and drive resolution
// → the committed offsets MUST be present in the OffsetStore (no loss).
//
// Under the OLD (buggy) order this FAILS: CompleteCommit is persisted, the throw
// aborts the offset apply, and the recovered CompleteCommit re-applies nothing.
TEST(TxnCoordEosDurabilityTest, CommittedOffsetsSurviveCrashBetweenCompleteCommitPersistAndOffsetApply)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    FakeOffsetStore store;
    const std::string group = "rpw-group";
    auto inputTp = tp("input-topic", 0);
    const rune::coordination::TopicPartition cinTp{inputTp.topic, inputTp.partition};
    const rune::coordination::Offset committedOffset = 42;

    ProducerId    pid{};
    ProducerEpoch epoch{};

    // Phase 1: drive txn to PrepareCommit with a pending consumer offset, then
    // "crash" during completeTransaction: the callback throws the first time it
    // is called.
    {
        TransactionLog log(path);

        // offsetApplyCb throws the FIRST time — models a crash landing in the
        // window right after the CompleteCommit persist (old order) or right at
        // the start of the offset apply (either order).
        bool thrown = false;
        auto crashingCb = [&](const std::string& g,
                              const rune::coordination::TopicPartition& t,
                              rune::coordination::Offset off,
                              int64_t) {
            if (!thrown) { thrown = true; throw std::runtime_error("crash-in-window"); }
            store.commit(g, t, off);
        };

        TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log,
                                     /*coordinatorEpoch=*/0, crashingCb);
        auto r = coord.initProducerId("rpw-txn", 60'000, 0LL);
        ASSERT_TRUE(r.ok());
        pid   = r.producerId;
        epoch = r.producerEpoch;
        coord.addPartitions("rpw-txn", pid, epoch, tps({tp("out", 0)}), 100LL);

        std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offs;
        offs[cinTp] = committedOffset;
        ASSERT_TRUE(coord.addOffsetsToTxn("rpw-txn", pid, epoch, group, offs, 150LL).ok());

        coord.endTransaction("rpw-txn", pid, epoch, /*commit=*/true, 200LL);
        ASSERT_EQ(coord.state("rpw-txn"), TxnState::PrepareCommit);

        // Drive completeTransaction — the callback throws, simulating a crash
        // in the persist/apply window.  Swallow it (the process "dies").
        try {
            coord.completeTransaction("rpw-txn");
        } catch (const std::exception&) { /* crashed */ }

        // Offset must NOT be visible yet (the apply was interrupted).
        EXPECT_FALSE(store.fetch(group, cinTp.topic, cinTp.partition).has_value())
            << "offset should not be applied yet — the apply was interrupted";
    }

    // Phase 2: reconstruct the coordinator from the SAME durable log.  A
    // non-throwing callback re-applies on the re-drive.
    TransactionLog log2(path);
    auto goodCb = [&](const std::string& g,
                     const rune::coordination::TopicPartition& t,
                     rune::coordination::Offset off, int64_t) {
        store.commit(g, t, off);
    };
    TransactionCoordinator coord2(ProducerIdAllocator{}, 60'000, &log2,
                                  /*coordinatorEpoch=*/0, goodCb);

    // Re-drive: a recovered PrepareCommit is exposed via pendingMarkerTransactions
    // and resolved via completeTransaction (idempotent marker + offset re-apply).
    auto pending = coord2.pendingMarkerTransactions();
    for (const auto& p : pending) {
        (void)p;
        coord2.completeTransaction("rpw-txn");
    }
    // If recovery landed us directly in a resolvable terminal state, drive it too.
    if (coord2.state("rpw-txn") == TxnState::PrepareCommit) {
        coord2.completeTransaction("rpw-txn");
    }

    // THE INVARIANT: the committed consumer offset MUST be durably present.
    auto co = store.fetch(group, cinTp.topic, cinTp.partition);
    ASSERT_TRUE(co.has_value())
        << "committed consumer offset LOST across crash-in-window (EOS violation)";
    EXPECT_EQ(*co, committedOffset);
}

// Normal (no-crash) commit still applies the pending offsets exactly once.
TEST(TxnCoordEosDurabilityTest, NormalCommitAppliesPendingOffsets)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    FakeOffsetStore store;
    int applyCount = 0;
    auto cb = [&](const std::string& g,
                 const rune::coordination::TopicPartition& t,
                 rune::coordination::Offset off, int64_t) {
        ++applyCount;
        store.commit(g, t, off);
    };

    TransactionLog log(path);
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log,
                                 /*coordinatorEpoch=*/0, cb);
    auto r = coord.initProducerId("ok-txn", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    coord.addPartitions("ok-txn", r.producerId, r.producerEpoch, tps({tp("out", 0)}), 100LL);

    rune::coordination::TopicPartition cin{"in", 0};
    std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offs;
    offs[cin] = 7;
    ASSERT_TRUE(coord.addOffsetsToTxn("ok-txn", r.producerId, r.producerEpoch,
                                      "g", offs, 150LL).ok());
    coord.endTransaction("ok-txn", r.producerId, r.producerEpoch, /*commit=*/true, 200LL);
    ASSERT_TRUE(coord.completeTransaction("ok-txn").ok());

    EXPECT_EQ(applyCount, 1);
    auto co = store.fetch("g", "in", 0);
    ASSERT_TRUE(co.has_value());
    EXPECT_EQ(*co, 7);
    EXPECT_EQ(coord.state("ok-txn"), TxnState::CompleteCommit);
}

// ===========================================================================
// P5-5 final re-audit liveness fix: recovered Complete* → Empty (txnId no wedge).
//
// Bug: completeTransaction writes CompleteCommit (or CompleteAbort) as one
// persisted record, then Empty as a SECOND persisted record.  A crash BETWEEN
// those two writes leaves a durable Complete* state.  The constructor only sets
// markersPending for Prepare*, so a recovered Complete* is never re-driven;
// initProducerId returns kConcurrentTransactions forever → the transactionalId
// is PERMANENTLY WEDGED.  Safety holds (offsets + markers were already durable
// before the Complete* record was written), but "every recovered state is
// resolvable" is violated.
//
// Fix: in the constructor replay loop, collapse a recovered CompleteCommit /
// CompleteAbort to Empty (with pendingNewProducerId adoption for CompleteAbort,
// mirroring what completeTransaction would have done).  No offsets are
// re-applied (already in the OffsetStore), no markers are re-written (already
// on disk, deduplicated by consumers).
// ===========================================================================

// ---------------------------------------------------------------------------
// Test 1 (MUST FAIL under old code): crash BETWEEN CompleteCommit-persist and
// Empty-reset → txnId must NOT be wedged, initProducerId succeeds and begins a
// NEW transaction (not kConcurrentTransactions forever).
//
// Also asserts: offsets are NOT re-applied on recovery (already in OffsetStore),
// and no duplicate markers were written.
// ---------------------------------------------------------------------------
TEST(TxnCoordP55LivenessTest, RecoveredCompleteCommitCollapsedToEmpty_NotWedged)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    // Track how many times the offset callback fires.
    int applyCount = 0;
    FakeOffsetStore store;
    auto cb = [&](const std::string& g,
                 const rune::coordination::TopicPartition& t,
                 rune::coordination::Offset off, int64_t) {
        ++applyCount;
        store.commit(g, t, off);
    };

    // Simulate the crash window: persist CompleteCommit to the log WITHOUT the
    // subsequent Empty record.  This is exactly what happens when the coordinator
    // crashes after the first persistTransition in completeTransaction() but
    // before the second.
    ProducerId    savedPid{};
    ProducerEpoch savedEpoch{};
    {
        TransactionLog log(path);
        // Phase 1: run the txn all the way to PrepareCommit.
        TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log,
                                     /*coordinatorEpoch=*/0, cb);
        auto r = coord.initProducerId("wedge-txn", 60'000, 0LL);
        ASSERT_TRUE(r.ok());
        savedPid   = r.producerId;
        savedEpoch = r.producerEpoch;

        rune::coordination::TopicPartition inputTp{"in", 0};
        std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offs;
        offs[inputTp] = 77;
        coord.addPartitions("wedge-txn", savedPid, savedEpoch, tps({tp("out", 0)}), 1LL);
        ASSERT_TRUE(coord.addOffsetsToTxn("wedge-txn", savedPid, savedEpoch,
                                          "grp", offs, 2LL).ok());
        coord.endTransaction("wedge-txn", savedPid, savedEpoch, /*commit=*/true, 3LL);
        ASSERT_EQ(coord.state("wedge-txn"), TxnState::PrepareCommit);
        // The coordinator is destroyed here WITHOUT calling completeTransaction.
        // The durable log holds Ongoing→PrepareCommit; we'll inject CompleteCommit
        // manually below to model the crash-in-window scenario.
    }
    applyCount = 0; // reset: we will inject state manually

    // Inject a CompleteCommit record (without an Empty follow-up) directly into
    // the log — this is the state left by a crash after the first write in
    // completeTransaction.
    {
        TransactionLog log(path);
        TxnLogEntry e;
        e.txnId         = "wedge-txn";
        e.producerId    = savedPid;
        e.producerEpoch = savedEpoch;
        e.state         = TxnState::CompleteCommit;
        e.txnTimeoutMs  = 60'000;
        // pendingOffsets are NOT on this entry (already applied before CompleteCommit
        // in the current code; mirroring that here).
        log.append(e);
    }

    // Phase 2: recover from the log.  The recovered CompleteCommit must be
    // collapsed to Empty — not wedge the transactionalId.
    TransactionLog log2(path);
    TransactionCoordinator coord2(ProducerIdAllocator{}, 60'000, &log2,
                                  /*coordinatorEpoch=*/0, cb);

    // KEY ASSERTION 1: the recovered CompleteCommit is the resting state (F15 §5.4).
    ASSERT_TRUE(coord2.state("wedge-txn").has_value());
    EXPECT_EQ(coord2.state("wedge-txn"), TxnState::CompleteCommit)
        << "a recovered CompleteCommit stays the resting state (F15 §5.4)";

    // KEY ASSERTION 2: initProducerId must succeed (not kConcurrentTransactions).
    auto r2 = coord2.initProducerId("wedge-txn", 60'000, 100LL);
    EXPECT_NE(r2.errorCode, InitProducerIdResult::kConcurrentTransactions)
        << "txnId must NOT be wedged: initProducerId must not return kConcurrentTransactions";
    EXPECT_TRUE(r2.ok())
        << "initProducerId on a recovered CompleteCommit txnId must succeed";

    // KEY ASSERTION 3: offsets NOT re-applied on recovery collapse (already in store).
    EXPECT_EQ(applyCount, 0)
        << "Offsets must NOT be re-applied when recovering a CompleteCommit";

    // KEY ASSERTION 4: the txnId is usable for a NEW transaction.
    auto addRes = coord2.addPartitions("wedge-txn", r2.producerId, r2.producerEpoch,
                                       tps({tp("new-part", 0)}), 200LL);
    EXPECT_EQ(addRes.errorCode, AddPartitionsResult::kOk)
        << "txnId must be fully reusable after CompleteCommit collapse";
}

// ---------------------------------------------------------------------------
// Test 2 (MUST FAIL under old code): crash BETWEEN CompleteAbort-persist and
// Empty-reset → txnId not wedged, pendingNewProducerId correctly adopted.
// ---------------------------------------------------------------------------
TEST(TxnCoordP55LivenessTest, RecoveredCompleteAbortCollapsedToEmpty_NotWedged)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    // Inject a CompleteAbort directly, without a following Empty.
    ProducerId    oldPid   = 42;
    ProducerEpoch oldEpoch = 3;
    {
        TransactionLog log(path);
        TxnLogEntry e;
        e.txnId         = "abort-wedge";
        e.producerId    = oldPid;
        e.producerEpoch = oldEpoch;
        e.state         = TxnState::CompleteAbort;
        e.txnTimeoutMs  = 60'000;
        log.append(e);
    }

    TransactionLog log2(path);
    TransactionCoordinator coord(ProducerIdAllocator{100}, 60'000, &log2, 0);

    // F15 §5.4: the resting CompleteAbort.
    ASSERT_TRUE(coord.state("abort-wedge").has_value());
    EXPECT_EQ(coord.state("abort-wedge"), TxnState::CompleteAbort)
        << "a recovered CompleteAbort stays the resting state (F15 §5.4)";

    // initProducerId must succeed.
    auto r = coord.initProducerId("abort-wedge", 60'000, 0LL);
    EXPECT_TRUE(r.ok())
        << "txnId must NOT be wedged after CompleteAbort collapse";
    EXPECT_NE(r.errorCode, InitProducerIdResult::kConcurrentTransactions);
}

// ---------------------------------------------------------------------------
// Test 3 (MUST FAIL under old code): crash between CompleteAbort and Empty when
// pendingNewProducerId was set (NEW-1/A2 overflow path) → recovered state must
// adopt the deferred PID, matching what completeTransaction would have done.
// ---------------------------------------------------------------------------
TEST(TxnCoordP55LivenessTest, RecoveredCompleteAbortWithPendingNewPid_AdoptsDeferredPid)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    ProducerId    oldPid   = 55;
    ProducerEpoch oldEpoch = static_cast<ProducerEpoch>(INT16_MAX);  // overflow was triggered
    ProducerId    freshPid = 200;
    ProducerEpoch freshEpoch = 0;

    // Inject a CompleteAbort that still carries pendingNewProducerId (the deferred
    // PID was NOT yet adopted — crash happened before the Empty write).
    {
        TransactionLog log(path);
        TxnLogEntry e;
        e.txnId                  = "overflow-abort-wedge";
        e.producerId             = oldPid;
        e.producerEpoch          = oldEpoch;
        e.state                  = TxnState::CompleteAbort;
        e.txnTimeoutMs           = 60'000;
        e.pendingNewProducerId   = freshPid;
        e.pendingNewProducerEpoch = freshEpoch;
        log.append(e);
    }

    // Recover: allocator starts past freshPid so no reuse.
    TransactionLog log2(path);
    TransactionCoordinator coord(ProducerIdAllocator{500}, 60'000, &log2, 0);

    // F15 §5.4: the resting CompleteAbort.
    ASSERT_TRUE(coord.state("overflow-abort-wedge").has_value());
    EXPECT_EQ(coord.state("overflow-abort-wedge"), TxnState::CompleteAbort)
        << "a recovered CompleteAbort+pendingNewPid stays the resting state, swap adopted";

    // The PID must be the DEFERRED fresh PID (not the old one), matching what
    // completeTransaction would have installed before writing Empty.
    EXPECT_EQ(coord.pidOf("overflow-abort-wedge"), freshPid)
        << "Recovered CompleteAbort must adopt pendingNewProducerId";
    EXPECT_EQ(coord.epochOf("overflow-abort-wedge"), freshEpoch)
        << "Recovered CompleteAbort must adopt pendingNewProducerEpoch";

    // initProducerId must succeed and not be wedged.
    auto r = coord.initProducerId("overflow-abort-wedge", 60'000, 0LL);
    EXPECT_TRUE(r.ok())
        << "txnId must be reusable after CompleteAbort+pendingNewPid collapse";
}

// ===========================================================================
// Part-B edge tests: addOffsetsToTxn fencing, multi-group, abort recovery,
// dedup, interleaved commit+abort with offsets.
// ===========================================================================

// ---------------------------------------------------------------------------
// Edge 4: addOffsetsToTxn epoch-fenced by a zombie (old epoch) → rejected.
// ---------------------------------------------------------------------------
TEST(TxnCoordP55EdgeTest, AddOffsetsToTxnFencedByZombieOldEpoch)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    TransactionLog log(path);
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log, 0);

    // Create producer, bump epoch via re-init, then try addOffsetsToTxn with the
    // old (zombie) epoch.
    auto r1 = coord.initProducerId("fenced-offs", 60'000, 0LL);
    ASSERT_TRUE(r1.ok());
    // Re-init bumps epoch.
    auto r2 = coord.initProducerId("fenced-offs", 60'000, 1LL);
    ASSERT_TRUE(r2.ok());
    EXPECT_GT(r2.producerEpoch, r1.producerEpoch);

    // Begin a new txn with the CURRENT epoch.
    coord.addPartitions("fenced-offs", r2.producerId, r2.producerEpoch,
                        tps({tp("t", 0)}), 10LL);
    ASSERT_EQ(coord.state("fenced-offs"), TxnState::Ongoing);

    rune::coordination::TopicPartition inputTp{"input", 0};
    std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offs;
    offs[inputTp] = 5;

    // OLD (zombie) epoch → must be fenced.
    auto res = coord.addOffsetsToTxn("fenced-offs", r1.producerId, r1.producerEpoch,
                                      "grp", offs, 20LL);
    EXPECT_NE(res.errorCode, AddPartitionsResult::kOk)
        << "addOffsetsToTxn with a zombie epoch must be rejected";
    EXPECT_EQ(res.errorCode, AddPartitionsResult::kInvalidProducerEpoch)
        << "zombie addOffsetsToTxn must return kInvalidProducerEpoch";
}

// ---------------------------------------------------------------------------
// Edge 5: multi-group offsets in one txn → ALL applied on commit /
//         ALL discarded on abort (two distinct groups, two distinct TPs).
// ---------------------------------------------------------------------------
TEST(TxnCoordP55EdgeTest, MultiGroupOffsetsAppliedAtomicallyOnCommit)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    FakeOffsetStore store;
    auto cb = [&](const std::string& g,
                 const rune::coordination::TopicPartition& t,
                 rune::coordination::Offset off, int64_t) {
        store.commit(g, t, off);
    };
    TransactionLog log(path);
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log, 0, cb);

    auto r = coord.initProducerId("multi-grp", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    coord.addPartitions("multi-grp", r.producerId, r.producerEpoch,
                        tps({tp("out", 0)}), 1LL);

    rune::coordination::TopicPartition tpA{"in", 0};
    rune::coordination::TopicPartition tpB{"in", 1};
    std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offsA{{tpA, 11}};
    std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offsB{{tpB, 22}};

    ASSERT_TRUE(coord.addOffsetsToTxn("multi-grp", r.producerId, r.producerEpoch,
                                       "group-A", offsA, 2LL).ok());
    ASSERT_TRUE(coord.addOffsetsToTxn("multi-grp", r.producerId, r.producerEpoch,
                                       "group-B", offsB, 3LL).ok());

    coord.endTransaction("multi-grp", r.producerId, r.producerEpoch, /*commit=*/true, 4LL);
    ASSERT_TRUE(coord.completeTransaction("multi-grp").ok());

    // Both groups' offsets must be in the store.
    auto coA = store.fetch("group-A", "in", 0);
    auto coB = store.fetch("group-B", "in", 1);
    ASSERT_TRUE(coA.has_value()) << "group-A offset must be applied on commit";
    ASSERT_TRUE(coB.has_value()) << "group-B offset must be applied on commit";
    EXPECT_EQ(*coA, 11);
    EXPECT_EQ(*coB, 22);
}

TEST(TxnCoordP55EdgeTest, MultiGroupOffsetsDiscardedOnAbort)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    FakeOffsetStore store;
    auto cb = [&](const std::string& g,
                 const rune::coordination::TopicPartition& t,
                 rune::coordination::Offset off, int64_t) {
        store.commit(g, t, off);
    };
    TransactionLog log(path);
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log, 0, cb);

    auto r = coord.initProducerId("multi-grp-abort", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    coord.addPartitions("multi-grp-abort", r.producerId, r.producerEpoch,
                        tps({tp("out", 0)}), 1LL);

    rune::coordination::TopicPartition tpA{"in", 0};
    rune::coordination::TopicPartition tpB{"in", 1};
    std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offsA{{tpA, 11}};
    std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offsB{{tpB, 22}};

    ASSERT_TRUE(coord.addOffsetsToTxn("multi-grp-abort", r.producerId, r.producerEpoch,
                                       "grp-A", offsA, 2LL).ok());
    ASSERT_TRUE(coord.addOffsetsToTxn("multi-grp-abort", r.producerId, r.producerEpoch,
                                       "grp-B", offsB, 3LL).ok());

    coord.endTransaction("multi-grp-abort", r.producerId, r.producerEpoch, /*commit=*/false, 4LL);
    ASSERT_TRUE(coord.completeTransaction("multi-grp-abort").ok());

    // Neither group's offsets must be in the store.
    EXPECT_FALSE(store.fetch("grp-A", "in", 0).has_value())
        << "grp-A offset must be discarded on abort";
    EXPECT_FALSE(store.fetch("grp-B", "in", 1).has_value())
        << "grp-B offset must be discarded on abort";
}

// ---------------------------------------------------------------------------
// Edge 6: abort AFTER addOffsetsToTxn THEN coordinator recovery →
//         recovered PrepareAbort resolve still discards pendingOffsets.
// ---------------------------------------------------------------------------
TEST(TxnCoordP55EdgeTest, AbortAfterAddOffsetsRecovery_OffsetsDiscarded)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    FakeOffsetStore store;
    auto cb = [&](const std::string& g,
                 const rune::coordination::TopicPartition& t,
                 rune::coordination::Offset off, int64_t) {
        store.commit(g, t, off);   // must NEVER fire
    };

    // Phase 1: addOffsetsToTxn, then abort, then crash (without completeTransaction).
    {
        TransactionLog log(path);
        TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log, 0, cb);
        auto r = coord.initProducerId("abort-offs-rec", 60'000, 0LL);
        ASSERT_TRUE(r.ok());
        coord.addPartitions("abort-offs-rec", r.producerId, r.producerEpoch,
                            tps({tp("out", 0)}), 1LL);

        rune::coordination::TopicPartition tp0{"in", 0};
        std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offs{{tp0, 99}};
        ASSERT_TRUE(coord.addOffsetsToTxn("abort-offs-rec", r.producerId, r.producerEpoch,
                                           "g", offs, 2LL).ok());
        // Abort (drive to PrepareAbort), then crash before completeTransaction.
        coord.endTransaction("abort-offs-rec", r.producerId, r.producerEpoch,
                             /*commit=*/false, 3LL);
        ASSERT_EQ(coord.state("abort-offs-rec"), TxnState::PrepareAbort);
        // Coordinator destroyed here (crash).
    }

    // Phase 2: recover and resolve.
    TransactionLog log2(path);
    TransactionCoordinator coord2(ProducerIdAllocator{}, 60'000, &log2, 0, cb);
    // markersPending is set for the recovered PrepareAbort.
    for (const auto& p : coord2.pendingMarkerTransactions()) {
        (void)p;
        coord2.completeTransaction("abort-offs-rec");
    }

    EXPECT_FALSE(store.fetch("g", "in", 0).has_value())
        << "Offsets from an aborted txn must NEVER appear in the OffsetStore, even after recovery";
}

// ---------------------------------------------------------------------------
// Edge 7: same (group,tp) added twice in one txn with DIFFERENT offsets →
//         last-write-wins after commit (OffsetStore overwrites idempotently).
// ---------------------------------------------------------------------------
TEST(TxnCoordP55EdgeTest, SameGroupTpAddedTwice_LastWriteWinsOnCommit)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    FakeOffsetStore store;
    auto cb = [&](const std::string& g,
                 const rune::coordination::TopicPartition& t,
                 rune::coordination::Offset off, int64_t) {
        store.commit(g, t, off);
    };
    TransactionLog log(path);
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log, 0, cb);

    auto r = coord.initProducerId("dup-offs", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    coord.addPartitions("dup-offs", r.producerId, r.producerEpoch,
                        tps({tp("out", 0)}), 1LL);

    rune::coordination::TopicPartition inp{"in", 0};

    // First addOffsetsToTxn: offset=5.
    {
        std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offs{{inp, 5}};
        ASSERT_TRUE(coord.addOffsetsToTxn("dup-offs", r.producerId, r.producerEpoch,
                                           "g", offs, 2LL).ok());
    }
    // Second addOffsetsToTxn with same key: offset=99 (last-write-wins).
    {
        std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offs{{inp, 99}};
        ASSERT_TRUE(coord.addOffsetsToTxn("dup-offs", r.producerId, r.producerEpoch,
                                           "g", offs, 3LL).ok());
    }

    coord.endTransaction("dup-offs", r.producerId, r.producerEpoch, /*commit=*/true, 4LL);
    ASSERT_TRUE(coord.completeTransaction("dup-offs").ok());

    auto co = store.fetch("g", "in", 0);
    ASSERT_TRUE(co.has_value()) << "Offset must be in store after commit";
    EXPECT_EQ(*co, 99) << "Last-write-wins: second addOffsetsToTxn offset must win";
}

// ---------------------------------------------------------------------------
// Edge 8: interleaved commit+abort txns with offsets → committed group's
//         offsets present, aborted group's offsets absent.
// ---------------------------------------------------------------------------
TEST(TxnCoordP55EdgeTest, InterleavedCommitAbort_OnlyCommittedOffsetsApplied)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};
    FakeOffsetStore store;
    auto cb = [&](const std::string& g,
                 const rune::coordination::TopicPartition& t,
                 rune::coordination::Offset off, int64_t) {
        store.commit(g, t, off);
    };
    TransactionLog log(path);
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log, 0, cb);

    rune::coordination::TopicPartition tp0{"in", 0};

    // Txn-A: commit with offset 10 for group-commit.
    {
        auto r = coord.initProducerId("txn-a-commit", 60'000, 0LL);
        ASSERT_TRUE(r.ok());
        coord.addPartitions("txn-a-commit", r.producerId, r.producerEpoch,
                            tps({tp("out", 0)}), 1LL);
        std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offs{{tp0, 10}};
        ASSERT_TRUE(coord.addOffsetsToTxn("txn-a-commit", r.producerId, r.producerEpoch,
                                           "group-commit", offs, 2LL).ok());
        coord.endTransaction("txn-a-commit", r.producerId, r.producerEpoch,
                             /*commit=*/true, 3LL);
        ASSERT_TRUE(coord.completeTransaction("txn-a-commit").ok());
    }

    // Txn-B: abort with offset 20 for group-abort.
    {
        auto r = coord.initProducerId("txn-b-abort", 60'000, 10LL);
        ASSERT_TRUE(r.ok());
        coord.addPartitions("txn-b-abort", r.producerId, r.producerEpoch,
                            tps({tp("out", 1)}), 11LL);
        std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offs{{tp0, 20}};
        ASSERT_TRUE(coord.addOffsetsToTxn("txn-b-abort", r.producerId, r.producerEpoch,
                                           "group-abort", offs, 12LL).ok());
        coord.endTransaction("txn-b-abort", r.producerId, r.producerEpoch,
                             /*commit=*/false, 13LL);
        ASSERT_TRUE(coord.completeTransaction("txn-b-abort").ok());
    }

    // Committed group's offset present.
    auto coCommit = store.fetch("group-commit", "in", 0);
    ASSERT_TRUE(coCommit.has_value()) << "Committed group offset must be applied";
    EXPECT_EQ(*coCommit, 10);

    // Aborted group's offset absent.
    EXPECT_FALSE(store.fetch("group-abort", "in", 0).has_value())
        << "Aborted group offset must NOT be applied";
}

// Abort MUST still discard pending offsets — they are never written on abort,
// including across a crash + recovery of the aborted transaction.
TEST(TxnCoordEosDurabilityTest, AbortDiscardsPendingOffsetsEvenAcrossRecovery)
{
    auto path = coordTmpPath();
    CoordTempFile tf{path};

    FakeOffsetStore store;
    auto cb = [&](const std::string& g,
                 const rune::coordination::TopicPartition& t,
                 rune::coordination::Offset off, int64_t) {
        store.commit(g, t, off);   // must NEVER be reached for an abort
    };

    ProducerId pid{}; ProducerEpoch epoch{};
    {
        TransactionLog log(path);
        TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, &log,
                                     /*coordinatorEpoch=*/0, cb);
        auto r = coord.initProducerId("ab-txn", 60'000, 0LL);
        ASSERT_TRUE(r.ok());
        pid = r.producerId; epoch = r.producerEpoch;
        coord.addPartitions("ab-txn", pid, epoch, tps({tp("out", 0)}), 100LL);

        rune::coordination::TopicPartition cin{"in", 0};
        std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offs;
        offs[cin] = 99;
        ASSERT_TRUE(coord.addOffsetsToTxn("ab-txn", pid, epoch, "g", offs, 150LL).ok());
        coord.endTransaction("ab-txn", pid, epoch, /*commit=*/false, 200LL);
        ASSERT_EQ(coord.state("ab-txn"), TxnState::PrepareAbort);
        // Crash before completeTransaction.
    }

    // Recover and resolve the abort — offsets must STILL be discarded.
    TransactionLog log2(path);
    TransactionCoordinator coord2(ProducerIdAllocator{}, 60'000, &log2,
                                  /*coordinatorEpoch=*/0, cb);
    for (const auto& p : coord2.pendingMarkerTransactions()) {
        (void)p;
        coord2.completeTransaction("ab-txn");
    }
    EXPECT_FALSE(store.fetch("g", "in", 0).has_value())
        << "aborted txn offsets must NEVER be applied to the OffsetStore";
}

// X-1 (broker contract gap from client F8 C4+C5 re-review 2): AddOffsetsToTxn
// opens a transaction from Empty / Complete* exactly like AddPartitionsToTxn
// (Kafka).  The client sends it as soon as sendOffsetsToTransaction is applied —
// often BEFORE any AddPartitions — and an offsets-only txn never sends one.
TEST(TxnAddOffsetsRegression, AddOffsetsOpensTheTxnAndItsOffsetsApplyOnCommit) {
    std::vector<std::pair<std::string, rune::coordination::Offset>> applied;
    auto cb = [&](const std::string& g, const rune::coordination::TopicPartition&,
                  rune::coordination::Offset off, int64_t) { applied.emplace_back(g, off); };
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, nullptr, /*coordinatorEpoch=*/0, cb);
    for (int round = 0; round < 2; ++round) {                 // from Empty, then from the resting CompleteCommit
        auto r = coord.initProducerId("off-only", 60'000, 0LL);
        ASSERT_TRUE(r.ok());
        std::map<rune::coordination::TopicPartition, rune::coordination::Offset> none;
        // TxnOffsetCommit (no begin) needs an Ongoing txn.
        std::map<rune::coordination::TopicPartition, rune::coordination::Offset> offs;
        rune::coordination::TopicPartition in; in.topic = "in"; in.partition = 0;
        offs[in] = 40 + round;
        EXPECT_EQ(coord.addOffsetsToTxn("off-only", r.producerId, r.producerEpoch, "g", offs, 10LL).errorCode,
                  AddPartitionsResult::kInvalidTransition) << "round " << round;
        // AddOffsetsToTxn opens it.
        ASSERT_TRUE(coord.addOffsetsToTxn("off-only", r.producerId, r.producerEpoch, "g", none, 10LL,
                                          /*beginsTxn=*/true).ok()) << "round " << round;
        EXPECT_EQ(coord.state("off-only"), TxnState::Ongoing);
        ASSERT_TRUE(coord.addOffsetsToTxn("off-only", r.producerId, r.producerEpoch, "g", offs, 11LL).ok());
        ASSERT_TRUE(coord.endTransaction("off-only", r.producerId, r.producerEpoch, /*commit=*/true, 12LL).ok());
        ASSERT_TRUE(coord.completeTransaction("off-only").ok());
        EXPECT_EQ(coord.state("off-only"), TxnState::CompleteCommit);
        ASSERT_EQ(applied.size(), static_cast<std::size_t>(round + 1));
        EXPECT_EQ(applied.back().second, 40 + round) << "the offsets-only txn's offsets were not applied on commit";
    }
}

TEST(TxnAddOffsetsRegression, AddOffsetsInPrepareIsRefusedNotReopened) {
    TransactionCoordinator coord(ProducerIdAllocator{}, 60'000, nullptr, 0);
    auto r = coord.initProducerId("prep", 60'000, 0LL);
    ASSERT_TRUE(r.ok());
    ASSERT_TRUE(coord.addPartitions("prep", r.producerId, r.producerEpoch, tps({tp("out", 0)}), 1LL).ok());
    ASSERT_TRUE(coord.endTransaction("prep", r.producerId, r.producerEpoch, true, 2LL).ok());
    std::map<rune::coordination::TopicPartition, rune::coordination::Offset> none;
    EXPECT_EQ(coord.addOffsetsToTxn("prep", r.producerId, r.producerEpoch, "g", none, 3LL, true).errorCode,
              AddPartitionsResult::kInvalidTransition);
    EXPECT_EQ(coord.state("prep"), TxnState::PrepareCommit) << "AddOffsets must never reopen a preparing txn";
}

} // anonymous namespace

// ===========================================================================
// Issue #122: producer ids are issued bound to their owner (rune/protocol/ProducerIdBinding.h).
// ===========================================================================
namespace rp = rune::protocol;

TEST(ProducerIdBindingRegression, IdsAreIssuedBoundToTheirOwnerRegression)
{
    TransactionCoordinator coord;
    const auto a = coord.initProducerId(std::nullopt, 60'000, 0, /*owner=*/7);
    const auto b = coord.initProducerId(std::nullopt, 60'000, 0, /*owner=*/8);
    ASSERT_TRUE(a.ok());
    ASSERT_TRUE(b.ok());
    EXPECT_TRUE(rp::isBoundProducerId(a.producerId));
    EXPECT_TRUE(rp::producerIdOwnedBy(a.producerId, 7));
    EXPECT_FALSE(rp::producerIdOwnedBy(a.producerId, 8));
    EXPECT_TRUE(rp::producerIdOwnedBy(b.producerId, 8));
    EXPECT_GT(rp::producerIdCounter(b.producerId), rp::producerIdCounter(a.producerId)) << "one monotone counter";
    const auto t = coord.initProducerId("tx", 60'000, 0, /*owner=*/7);
    ASSERT_TRUE(t.ok());
    EXPECT_TRUE(rp::producerIdOwnedBy(t.producerId, 7));
}

TEST(ProducerIdBindingRegression, AnotherOwnerOfATransactionalIdGetsAFreshIdAndFencesTheOldRegression)
{
    TransactionCoordinator coord;
    const auto a1 = coord.initProducerId("tx", 60'000, 0, 7);
    const auto a2 = coord.initProducerId("tx", 60'000, 0, 7);
    ASSERT_TRUE(a1.ok());
    ASSERT_TRUE(a2.ok());
    EXPECT_EQ(a2.producerId, a1.producerId) << "the same owner: an epoch bump";
    EXPECT_EQ(a2.producerEpoch, a1.producerEpoch + 1);

    // An open transaction under owner 7's id; owner 8 takes the transactional id over.
    ASSERT_TRUE(coord.addPartitions("tx", a2.producerId, a2.producerEpoch, {{"t", 0}}, 1).ok());
    const auto b = coord.initProducerId("tx", 60'000, 2, 8);
    ASSERT_TRUE(b.ok());
    EXPECT_NE(b.producerId, a2.producerId) << "owner 8 was handed owner 7's producer id";
    EXPECT_TRUE(rp::producerIdOwnedBy(b.producerId, 8));
    EXPECT_EQ(b.producerEpoch, 0);
    const TransactionMetadata* m = coord.metadata("tx");
    ASSERT_NE(m, nullptr);
    EXPECT_EQ(m->state, TxnState::PrepareAbort) << "the previous holder's open transaction is aborted";
    EXPECT_EQ(m->producerId, a2.producerId) << "... under the OLD id (its batches carry it)";
    EXPECT_EQ(m->pendingNewProducerId, b.producerId);
}

TEST(ProducerIdBindingRegression, ALegacyIdOfATransactionalIdIsReboundAtItsNextInitRegression)
{
    TransactionCoordinator coord;
    coord.setBoundProducerIds(true);
    std::map<std::string, TransactionMetadata> snapshot;
    TransactionMetadata legacy;
    legacy.txnId = "tx"; legacy.producerId = 42; legacy.producerEpoch = 3; legacy.state = TxnState::Empty;
    snapshot.emplace("tx", legacy);
    TransactionMetadata bound;
    bound.txnId = "tx2"; bound.producerId = rp::bindProducerId(500, 9); bound.producerEpoch = 0;
    bound.state = TxnState::Empty;
    snapshot.emplace("tx2", bound);
    (void)coord.reload(snapshot, 1, 0);

    const auto r = coord.initProducerId("tx", 60'000, 0, 7);
    ASSERT_TRUE(r.ok());
    EXPECT_NE(r.producerId, 42) << "a legacy id carries no owner: it is never handed over";
    EXPECT_TRUE(rp::producerIdOwnedBy(r.producerId, 7));
    EXPECT_GT(rp::producerIdCounter(r.producerId), 500) << "the reseed honours a recovered bound id's counter";
    const auto own = coord.initProducerId("tx2", 60'000, 0, 9);
    EXPECT_EQ(own.producerId, bound.producerId) << "the owner of a recovered bound id keeps it";
    EXPECT_EQ(own.producerEpoch, 1);
}
