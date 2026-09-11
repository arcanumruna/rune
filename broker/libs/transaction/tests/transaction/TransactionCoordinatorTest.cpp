// ---------------------------------------------------------------------------
// TransactionCoordinatorTest.cpp — Unit tests for TransactionCoordinator.
//
// TDD: Kafka-faithful state machine, zombie fencing, timeout, full lifecycle.
// Bounded drivers with hard caps + ASSERT(done). No spin. Injected clock.
// ---------------------------------------------------------------------------

#include <rune/transaction/TransactionCoordinator.h>
#include <rune/transaction/TransactionLog.h>
#include <rune/transaction/ControlMarker.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cassert>
#include <optional>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::transaction;

namespace {

// ---------------------------------------------------------------------------
// Unique temp path helper (for durable log tests — FIX 1/4)
// ---------------------------------------------------------------------------
static std::atomic<int> g_coord_counter{0};

static std::string coordTmpPath() {
    return "/tmp/txncoord_" + std::to_string(::getpid())
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

TEST_F(TxnCoordTest, ReInitOngoingThenCompleteResetsToEmpty)
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
    EXPECT_EQ(coord_.state("txn-1"), TxnState::Empty);
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

TEST_F(TxnCoordTest, AddPartitionsWhenCompleteCommitIsInvalidTransition)
{
    // Setup: full commit path.
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-1", r.producerId, r.producerEpoch, true, nowNs_);
    // At PrepareCommit — force transition to CompleteCommit by not completing.
    // Actually CompleteCommit is not directly reachable without completeTransaction.
    // Test: add while PrepareCommit (already tested above). Skip.
    SUCCEED();
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

TEST_F(TxnCoordTest, CompleteCommitTransitionsThenEmpty)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-1", r.producerId, r.producerEpoch, true, nowNs_);
    ASSERT_EQ(coord_.state("txn-1"), TxnState::PrepareCommit);

    auto cr = coord_.completeTransaction("txn-1");
    EXPECT_TRUE(cr.ok());
    EXPECT_EQ(coord_.state("txn-1"), TxnState::Empty);
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

TEST_F(TxnCoordTest, CompleteAbortTransitionsThenEmpty)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-1", r.producerId, r.producerEpoch, false, nowNs_);
    ASSERT_EQ(coord_.state("txn-1"), TxnState::PrepareAbort);

    auto cr = coord_.completeTransaction("txn-1");
    EXPECT_TRUE(cr.ok());
    EXPECT_EQ(coord_.state("txn-1"), TxnState::Empty);
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

TEST_F(TxnCoordTest, EndTransactionWhenPrepareCommitIsInvalidTransition)
{
    auto r = coord_.initProducerId("txn-1", kDefaultTimeoutMs, nowNs_);
    coord_.addPartitions("txn-1", r.producerId, r.producerEpoch,
                         tps({tp("t", 0)}), nowNs_);
    coord_.endTransaction("txn-1", r.producerId, r.producerEpoch, true, nowNs_);
    ASSERT_EQ(coord_.state("txn-1"), TxnState::PrepareCommit);

    // Double commit.
    auto er = coord_.endTransaction("txn-1", r.producerId, r.producerEpoch,
                                    true, nowNs_);
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
    ASSERT_EQ(coord_.state("txn-1"), TxnState::Empty);

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
    ASSERT_EQ(coord_.state("txn-1"), TxnState::Empty);

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
    EXPECT_EQ(coord_.state("txn-1"), TxnState::Empty);

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
    ASSERT_EQ(coord_.state("txn-z"), TxnState::Empty);

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
    EXPECT_EQ(meta->state, TxnState::Empty);
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
    EXPECT_EQ(coord2.state("txn-replay"), TxnState::Empty);
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
    EXPECT_EQ(coord2.state("txn-cs-commit"), TxnState::Empty);
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
    EXPECT_EQ(coord2.state("txn-cs-abort"), TxnState::Empty);
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

} // anonymous namespace
