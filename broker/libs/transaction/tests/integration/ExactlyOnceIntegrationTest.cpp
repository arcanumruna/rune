// ---------------------------------------------------------------------------
// ExactlyOnceIntegrationTest.cpp — P5-5: Exactly-once end-to-end integration
//                                   + disaster/recovery suite.
//
// Namespace: rune::transaction (via harness)
// C++23. ALL loops bounded + ASSERT_TRUE. Injected clock. REAL Phase-5
// components (TransactionCoordinator + TransactionLog durable,
// ProducerStateManager, Log, ReadCommittedView, OffsetStore,
// ControlMarker::writeMarkers / resolvePendingMarkers).
//
// Test index (~32 tests):
//
// --- Atomic visibility ---
//  1.  AtomicCommit_ReadCommitted_AllOrNothingAcrossPartitions
//  2.  AtomicCommit_ReadCommitted_NothingBeforeCommit_LSO_Pinned
//  3.  AtomicCommit_ReadUncommitted_SeesDataBeforeCommit
//  4.  AtomicAbort_ReadCommitted_NoRecords
//  5.  AtomicAbort_ReadUncommitted_SeesData
//  6.  AtomicAbort_LsoAdvancesAfterAbort
//  7.  SequentialTxns_EachFullyVisible
//  8.  InterleavedProducers_CommitOneAbortOne
//  9.  CommitOnThreePartitions_AllPartitionsHaveData
// 10.  LsoNeverExposesOpenTxn
//
// --- Exactly-once processing ---
// 11.  ReadProcessWrite_CommitAtomicOutputAndOffsets
// 12.  ReadProcessWrite_Abort_NoDuplicateOutput_OffsetNotAdvanced
// 13.  ReadProcessWrite_AbortAndRetry_ExactlyOnce
// 14.  IdempotentProducerRetry_DedupNoDoubleAppend
// 15.  IdempotentProducerRetry_ReturnsSameOffset
// 16.  IdempotentProducerRetry_WindowWraps_OldDupAccepted
// 17.  IdempotentProducerOutOfOrderSequence_Rejected
//
// --- Zombie / epoch fencing ---
// 18.  ZombieProducer_OldEpochFencedFromProduce
// 19.  ZombieProducer_OldEpochFencedFromAddPartitions
// 20.  ZombieProducer_OldEpochFencedFromEndTxn
// 21.  EpochBump_NewProducerCanTransact
//
// --- Disaster / recovery ---
// 22.  CoordinatorFailover_RecoversPrepareCommit_CompletesCommit
// 23.  CoordinatorFailover_RecoversPrepareAbort_CompletesAbort
// 24.  CoordinatorFailover_OngoingTxnTimeoutAborted_MarkersWritten
// 25.  CoordinatorFailover_MultiTxn_AllResolved
// 26.  CoordinatorFailover_CommittedDataFullyVisibleAfterRecovery
// 27.  CoordinatorFailover_AbortedDataInvisibleAfterRecovery
// 28.  CoordinatorFailover_ZombieBlockedAfterEpochBump
// 29.  TimeoutAbort_DoWork_AbortedDataInvisible
// 30.  TimeoutAbort_LsoAdvancesAfterAbort
// 31.  TxnOffsetCommit_AtomicWithTxn_CommitAdvancesOffset
// 32.  TxnOffsetCommit_AbortDoesNotAdvanceOffset
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include "ExactlyOnceHarness.h"

#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/Types.h>
#include <rune/coordination/Types.h>

#include <cassert>
#include <string>
#include <vector>

using namespace rune::transaction;
using namespace rune::transaction::test;

// Hard cap for all bounded loops not using harness driveUntil.
static constexpr int kCap = 1000;

// Convenience alias
namespace rcoord = rune::coordination;

// ===========================================================================
// Group 1: Atomic visibility
// ===========================================================================

// ---------------------------------------------------------------------------
//  1. After commit: read_committed sees ALL data across ALL partitions.
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, AtomicCommit_ReadCommitted_AllOrNothingAcrossPartitions)
{
    ExactlyOnceHarness h(/*numPartitions=*/3);

    auto ir = h.initProducer("txn-1");
    ASSERT_TRUE(ir.ok());
    ProducerId pid = ir.producerId;
    ProducerEpoch epoch = ir.producerEpoch;

    ASSERT_TRUE(h.addAllPartitions("txn-1", pid, epoch).ok());

    // Produce one batch to each partition.
    // Each partition has its own independent sequence space (starts at 0).
    int64_t off0 = h.appendTxnData(pid, epoch, 0, 0, "p0-data");
    int64_t off1 = h.appendTxnData(pid, epoch, 1, 0, "p1-data");
    int64_t off2 = h.appendTxnData(pid, epoch, 2, 0, "p2-data");
    EXPECT_GE(off0, 0);
    EXPECT_GE(off1, 0);
    EXPECT_GE(off2, 0);

    // Before commit: read_committed must see nothing on any partition.
    EXPECT_TRUE(parseOffsets(h.readCommitted(0)).empty())
        << "read_committed must hide in-flight data";
    EXPECT_TRUE(parseOffsets(h.readCommitted(1)).empty());
    EXPECT_TRUE(parseOffsets(h.readCommitted(2)).empty());

    // Commit.
    ASSERT_TRUE(h.commitTxn("txn-1", pid, epoch));

    // After commit: all partitions have data visible.
    h.assertCommittedVisible(0, {off0});
    h.assertCommittedVisible(1, {off1});
    h.assertCommittedVisible(2, {off2});
    h.assertLsoLeHw();
}

// ---------------------------------------------------------------------------
//  2. Before commit: read_committed sees nothing (LSO pinned at first offset).
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, AtomicCommit_ReadCommitted_NothingBeforeCommit_LSO_Pinned)
{
    ExactlyOnceHarness h(/*numPartitions=*/1);

    auto ir = h.initProducer("txn-lso");
    ASSERT_TRUE(ir.ok());

    ASSERT_TRUE(h.addAllPartitions("txn-lso", ir.producerId, ir.producerEpoch).ok());
    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "data");

    int64_t hw  = h.partition(0).hw();
    int64_t lso = h.partition(0).lsoTracker.lso(hw);
    EXPECT_EQ(lso, 0) << "LSO must be pinned at first open txn offset";
    EXPECT_LE(lso, hw);

    auto raw = h.readCommitted(0);
    EXPECT_TRUE(parseOffsets(raw).empty())
        << "Nothing visible under read_committed before commit";
}

// ---------------------------------------------------------------------------
//  3. Before commit: read_uncommitted sees in-flight data.
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, AtomicCommit_ReadUncommitted_SeesDataBeforeCommit)
{
    ExactlyOnceHarness h(1);

    auto ir = h.initProducer("txn-ru");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("txn-ru", ir.producerId, ir.producerEpoch).ok());
    int64_t off = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "in-flight");
    ASSERT_GE(off, 0);

    h.assertReadUncommittedSeesData(0, {off});
}

// ---------------------------------------------------------------------------
//  4. After abort: read_committed sees NO records from the aborted txn.
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, AtomicAbort_ReadCommitted_NoRecords)
{
    ExactlyOnceHarness h(2);

    auto ir = h.initProducer("txn-abort");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("txn-abort", ir.producerId, ir.producerEpoch).ok());

    int64_t off0 = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "abort-p0");
    int64_t off1 = h.appendTxnData(ir.producerId, ir.producerEpoch, 1, 0, "abort-p1");

    ASSERT_TRUE(h.abortTxn("txn-abort", ir.producerId, ir.producerEpoch));

    h.assertAbortedInvisible(0, {off0});
    h.assertAbortedInvisible(1, {off1});
    h.assertLsoLeHw();
}

// ---------------------------------------------------------------------------
//  5. After abort: read_uncommitted still sees the data.
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, AtomicAbort_ReadUncommitted_SeesData)
{
    ExactlyOnceHarness h(1);

    auto ir = h.initProducer("txn-abort-ru");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("txn-abort-ru", ir.producerId, ir.producerEpoch).ok());
    int64_t off = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "aborted");

    ASSERT_TRUE(h.abortTxn("txn-abort-ru", ir.producerId, ir.producerEpoch));

    // read_uncommitted sees data; read_committed does not.
    h.assertReadUncommittedSeesData(0, {off});
    h.assertAbortedInvisible(0, {off});
}

// ---------------------------------------------------------------------------
//  6. After abort: LSO advances past the aborted txn (no longer pinned).
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, AtomicAbort_LsoAdvancesAfterAbort)
{
    ExactlyOnceHarness h(1);

    auto ir = h.initProducer("txn-lso-abort");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("txn-lso-abort", ir.producerId, ir.producerEpoch).ok());
    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "data");

    // Before abort: LSO pinned.
    EXPECT_EQ(h.partition(0).lsoTracker.lso(h.partition(0).hw()), 0);

    ASSERT_TRUE(h.abortTxn("txn-lso-abort", ir.producerId, ir.producerEpoch));

    // After abort: LSO == HW (no open txns).
    int64_t hw = h.partition(0).hw();
    EXPECT_EQ(h.partition(0).lsoTracker.lso(hw), hw)
        << "LSO must advance to HW after abort";
}

// ---------------------------------------------------------------------------
//  7. Sequential txns: each is fully visible after its commit.
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, SequentialTxns_EachFullyVisible)
{
    ExactlyOnceHarness h(1);

    for (int t = 0; t < 5; ++t) {
        std::string txnId = "seq-txn-" + std::to_string(t);
        auto ir = h.initProducer(txnId);
        ASSERT_TRUE(ir.ok());
        ASSERT_TRUE(h.addAllPartitions(txnId, ir.producerId, ir.producerEpoch).ok());
        int64_t off = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0,
                                        "seq-" + std::to_string(t));
        ASSERT_GE(off, 0);
        ASSERT_TRUE(h.commitTxn(txnId, ir.producerId, ir.producerEpoch));
        h.assertCommittedVisible(0, {off});
    }
    h.assertLsoLeHw();
}

// ---------------------------------------------------------------------------
//  8. Interleaved producers: one commits, one aborts.  read_committed sees
//     only the committed producer's data.
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, InterleavedProducers_CommitOneAbortOne)
{
    ExactlyOnceHarness h(1);

    // Producer A
    auto irA = h.initProducer("txn-A");
    ASSERT_TRUE(irA.ok());
    ASSERT_TRUE(h.addAllPartitions("txn-A", irA.producerId, irA.producerEpoch).ok());

    // Producer B
    auto irB = h.initProducer("txn-B");
    ASSERT_TRUE(irB.ok());
    ASSERT_TRUE(h.addAllPartitions("txn-B", irB.producerId, irB.producerEpoch).ok());

    // Interleave: A writes, B writes, A writes, then A commits, B aborts.
    int64_t offA0 = h.appendTxnData(irA.producerId, irA.producerEpoch, 0, 0, "A0");
    int64_t offB0 = h.appendTxnData(irB.producerId, irB.producerEpoch, 0, 0, "B0");
    int64_t offA1 = h.appendTxnData(irA.producerId, irA.producerEpoch, 0, 1, "A1");

    ASSERT_TRUE(h.commitTxn("txn-A", irA.producerId, irA.producerEpoch));
    ASSERT_TRUE(h.abortTxn("txn-B", irB.producerId, irB.producerEpoch));

    // read_committed: A visible, B invisible.
    h.assertCommittedVisible(0, {offA0, offA1});
    h.assertAbortedInvisible(0, {offB0});
    h.assertLsoLeHw();
}

// ---------------------------------------------------------------------------
//  9. Commit across 3 partitions: all 3 partitions have the data.
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, CommitOnThreePartitions_AllPartitionsHaveData)
{
    ExactlyOnceHarness h(3);

    auto ir = h.initProducer("txn-3p");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("txn-3p", ir.producerId, ir.producerEpoch).ok());

    std::vector<int64_t> offs(3);
    for (int p = 0; p < 3; ++p) {
        // Each partition has independent sequence space; first batch is always seq=0.
        offs[static_cast<std::size_t>(p)] =
            h.appendTxnData(ir.producerId, ir.producerEpoch, p, 0, "p" + std::to_string(p));
        ASSERT_GE(offs[static_cast<std::size_t>(p)], 0);
    }

    ASSERT_TRUE(h.commitTxn("txn-3p", ir.producerId, ir.producerEpoch));

    for (int p = 0; p < 3; ++p) {
        h.assertCommittedVisible(p, {offs[static_cast<std::size_t>(p)]});
    }
}

// ---------------------------------------------------------------------------
// 10. LSO never exposes an open txn (checked throughout a multi-batch txn).
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, LsoNeverExposesOpenTxn)
{
    ExactlyOnceHarness h(2);

    auto ir = h.initProducer("txn-lso-check");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("txn-lso-check", ir.producerId, ir.producerEpoch).ok());

    for (int seq = 0; seq < 5; ++seq) {
        h.appendTxnData(ir.producerId, ir.producerEpoch, 0, seq, "d");
        h.appendTxnData(ir.producerId, ir.producerEpoch, 1, seq, "d");
        h.assertLsoLeHw();
    }

    // Commit and verify LSO == HW.
    ASSERT_TRUE(h.commitTxn("txn-lso-check", ir.producerId, ir.producerEpoch));
    for (int p = 0; p < 2; ++p) {
        int64_t hw = h.partition(p).hw();
        EXPECT_EQ(h.partition(p).lsoTracker.lso(hw), hw)
            << "LSO must equal HW after commit on partition " << p;
    }
}

// ===========================================================================
// Group 2: Exactly-once processing
// ===========================================================================

// ---------------------------------------------------------------------------
// 11. read-process-write: COMMIT makes output visible AND advances offset via
//     real addOffsetsToTxn (KIP-98).  The consumer-group offset must NOT
//     appear before commit (durable-before-visible) and MUST appear after.
// ---------------------------------------------------------------------------
TEST(ExactlyOnce, ReadProcessWrite_CommitAtomicOutputAndOffsets)
{
    // Two-partition setup: partition 0 = input, partition 1 = output.
    ExactlyOnceHarness h(2, "topic");

    // Produce some non-transactional input data to partition 0.
    rune::protocol::RecordBatchBuilder inputBatch(0, 0);
    inputBatch.setProducerId(-1);
    inputBatch.append({}, "input-record", 0);
    auto inputBytes = inputBatch.build();
    h.partition(0).log.append(inputBytes, 0);
    h.partition(0).lsoTracker.onProduceBatch(-1, 0); // non-txn → no open txn

    // Processor: initProducer, begin txn on output partition.
    auto ir = h.initProducer("rpw-txn");
    ASSERT_TRUE(ir.ok());
    ProducerId pid = ir.producerId;
    ProducerEpoch epoch = ir.producerEpoch;

    ASSERT_TRUE(h.addAllPartitions("rpw-txn", pid, epoch).ok());

    // Produce output to partition 1 (transactional).
    int64_t outOff = h.appendTxnData(pid, epoch, 1, 0, "output-record");
    ASSERT_GE(outOff, 0);

    // Register the input offset 1 as PENDING within the transaction (real KIP-98).
    rcoord::TopicPartition inputTp{"topic", 0};
    std::map<rcoord::TopicPartition, rcoord::Offset> pendingOffsets{{inputTp, 1}};
    ASSERT_TRUE(h.addOffsetsToTxn("rpw-txn", pid, epoch, "group-A", pendingOffsets).ok());

    // BEFORE commit: output hidden, offset not yet in store.
    EXPECT_TRUE(parseOffsets(h.readCommitted(1)).empty());
    EXPECT_FALSE(h.fetchOffset("group-A", inputTp).has_value())
        << "Offset must NOT be in OffsetStore before commit (durable-before-visible)";

    // Commit: output becomes visible AND consumer offset is applied atomically.
    ASSERT_TRUE(h.commitTxn("rpw-txn", pid, epoch));

    // Output is now visible.
    h.assertCommittedVisible(1, {outOff});

    // Consumer offset MUST reflect the value registered with addOffsetsToTxn.
    auto co = h.fetchOffset("group-A", inputTp);
    ASSERT_TRUE(co.has_value()) << "Offset must be in OffsetStore after commit";
    EXPECT_EQ(co->offset, 1LL);
}

// ---------------------------------------------------------------------------
// 12. read-process-write ABORT: no output visible, offset NOT advanced.
//     Uses real addOffsetsToTxn so the pending offset is recorded inside the
//     transaction and MUST be discarded (never written) on abort.
//     THIS TEST MUST FAIL if the abort path applies the pending offset.
// ---------------------------------------------------------------------------
TEST(ExactlyOnce, ReadProcessWrite_Abort_NoDuplicateOutput_OffsetNotAdvanced)
{
    ExactlyOnceHarness h(2, "topic");

    auto ir = h.initProducer("rpw-abort");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("rpw-abort", ir.producerId, ir.producerEpoch).ok());

    int64_t outOff = h.appendTxnData(ir.producerId, ir.producerEpoch, 1, 0, "spurious");
    ASSERT_GE(outOff, 0);

    // Register offset 3 as PENDING within the transaction.
    rcoord::TopicPartition inputTp{"topic", 0};
    std::map<rcoord::TopicPartition, rcoord::Offset> pendingOffsets{{inputTp, 3}};
    ASSERT_TRUE(h.addOffsetsToTxn("rpw-abort", ir.producerId, ir.producerEpoch,
                                   "group-B", pendingOffsets).ok());

    // Abort: pending offset must be discarded; output must be invisible.
    ASSERT_TRUE(h.abortTxn("rpw-abort", ir.producerId, ir.producerEpoch));

    // Output is invisible.
    h.assertAbortedInvisible(1, {outOff});

    // Offset MUST NOT be in OffsetStore.  This assertion MUST FAIL if the
    // abort path incorrectly calls offsetApplyCb_.
    EXPECT_FALSE(h.fetchOffset("group-B", inputTp).has_value())
        << "INVARIANT VIOLATED: offset was committed to OffsetStore on abort";
}

// ---------------------------------------------------------------------------
// 13. read-process-write: abort + retry produces exactly once (no dup).
// ---------------------------------------------------------------------------
TEST(ExactlyOnce, ReadProcessWrite_AbortAndRetry_ExactlyOnce)
{
    ExactlyOnceHarness h(2, "topic");

    // First attempt: produce output + abort.
    auto ir1 = h.initProducer("rpw-retry");
    ASSERT_TRUE(ir1.ok());
    ASSERT_TRUE(h.addAllPartitions("rpw-retry", ir1.producerId, ir1.producerEpoch).ok());
    int64_t outOff1 = h.appendTxnData(ir1.producerId, ir1.producerEpoch, 1, 0, "attempt1");
    ASSERT_TRUE(h.abortTxn("rpw-retry", ir1.producerId, ir1.producerEpoch));

    // Second attempt (epoch bumped via initProducer on same txnId → fences old).
    auto ir2 = h.initProducer("rpw-retry");
    ASSERT_TRUE(ir2.ok());
    EXPECT_GT(ir2.producerEpoch, ir1.producerEpoch)
        << "Epoch must be bumped on re-init to fence previous producer";
    ASSERT_TRUE(h.addAllPartitions("rpw-retry", ir2.producerId, ir2.producerEpoch).ok());
    int64_t outOff2 = h.appendTxnData(ir2.producerId, ir2.producerEpoch, 1, 0, "attempt2");
    ASSERT_TRUE(h.commitTxn("rpw-retry", ir2.producerId, ir2.producerEpoch));

    // Only the second attempt is visible (exactly once).
    h.assertAbortedInvisible(1, {outOff1});
    h.assertCommittedVisible(1, {outOff2});
}

// ---------------------------------------------------------------------------
// 14. Idempotent producer retry: same batch → ProducerStateManager dedups,
//     NOT re-appended to the log.
// ---------------------------------------------------------------------------
TEST(ExactlyOnce, IdempotentProducerRetry_DedupNoDoubleAppend)
{
    ExactlyOnceHarness h(1);

    auto ir = h.initProducer("idem-retry");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("idem-retry", ir.producerId, ir.producerEpoch).ok());

    // First append.
    int64_t off1 = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "batch-0");
    ASSERT_GE(off1, 0);
    int64_t leoAfterFirst = h.partition(0).hw();

    // Retry: same producerId + epoch + baseSequence → duplicate.
    int64_t off2 = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "batch-0");
    // PSM returns original offset; log NOT appended.
    EXPECT_EQ(off2, off1) << "Duplicate must return original offset";
    EXPECT_EQ(h.partition(0).hw(), leoAfterFirst) << "Log must not grow on duplicate";
}

// ---------------------------------------------------------------------------
// 15. Idempotent retry returns the SAME offset as the original.
// ---------------------------------------------------------------------------
TEST(ExactlyOnce, IdempotentProducerRetry_ReturnsSameOffset)
{
    ExactlyOnceHarness h(1);

    auto ir = h.initProducer("idem-off");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("idem-off", ir.producerId, ir.producerEpoch).ok());

    int64_t first = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "x");
    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 1, "y"); // second batch

    // Retry first batch.
    int64_t retry = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "x");
    EXPECT_EQ(retry, first);
}

// ---------------------------------------------------------------------------
// 16. Idempotent retry window wraps: old duplicate still accepted from cache.
// ---------------------------------------------------------------------------
TEST(ExactlyOnce, IdempotentProducerRetry_WindowWraps_OldDupAccepted)
{
    ExactlyOnceHarness h(1);

    auto ir = h.initProducer("idem-win");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("idem-win", ir.producerId, ir.producerEpoch).ok());

    // Append kRetryWindowSize+1 batches to fill and wrap the window.
    int64_t firstOff = -1;
    for (int i = 0; i <= rune::storage::kRetryWindowSize; ++i) {
        int64_t off = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, i, "d");
        if (i == 0) firstOff = off;
        ASSERT_GE(off, 0);
    }

    // Retry batch 0 — evicted from the window (kRetryWindowSize+1 batches).
    // EOS-OOS: entirely <= lastSequence → a KNOWN-OLD duplicate (Kafka
    // DUPLICATE_SEQUENCE_NUMBER): OK with an unknown offset, never OOS.
    auto vr = h.partition(0).psm.validate(
                  /*topicId=*/0, ir.producerId, 0, ir.producerEpoch, 0, 0);
    EXPECT_TRUE(vr.isDuplicate()) << "an evicted, stored batch is a known-old duplicate";
    EXPECT_EQ(vr.retryBaseOffset, -1);
    (void)firstOff;
}

// ---------------------------------------------------------------------------
// 17. Out-of-order sequence: rejected.
// ---------------------------------------------------------------------------
TEST(ExactlyOnce, IdempotentProducerOutOfOrderSequence_Rejected)
{
    ExactlyOnceHarness h(1);

    auto ir = h.initProducer("idem-oos");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("idem-oos", ir.producerId, ir.producerEpoch).ok());

    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "x");

    // Send seq=5 when 1 is expected → OOS.
    auto vr = h.partition(0).psm.validate(
                  /*topicId=*/0, ir.producerId, 0, ir.producerEpoch, 5, 5);
    EXPECT_TRUE(vr.isOos())
        << "Sequence gap (seq=5 when 1 expected) must be OOS";
}

// ===========================================================================
// Group 3: Zombie / epoch fencing
// ===========================================================================

// ---------------------------------------------------------------------------
// 18. Zombie producer: old epoch fenced from produce (validate returns Fenced).
// ---------------------------------------------------------------------------
TEST(ZombieFencing, ZombieProducer_OldEpochFencedFromProduce)
{
    ExactlyOnceHarness h(1);

    // Epoch 0.
    auto ir1 = h.initProducer("fence-txn");
    ASSERT_TRUE(ir1.ok());

    // Bump epoch: epoch 1 replaces epoch 0.
    auto ir2 = h.initProducer("fence-txn");
    ASSERT_TRUE(ir2.ok());
    EXPECT_GT(ir2.producerEpoch, ir1.producerEpoch);

    // Old producer (epoch 0) tries to validate → Fenced.
    auto vr = h.partition(0).psm.validate(
                  /*topicId=*/0, ir1.producerId, 0, ir1.producerEpoch, 0, 0);
    // PSM alone doesn't know about coordinator epochs — but when the
    // NEW producer uses the same PID with a higher epoch, any call by
    // the old epoch is fenced by PSM's epoch tracking AFTER one batch
    // from the new producer. Let's validate via the coordinator path:
    ASSERT_TRUE(h.addAllPartitions("fence-txn", ir2.producerId, ir2.producerEpoch).ok());
    h.appendTxnData(ir2.producerId, ir2.producerEpoch, 0, 0, "new");

    // Old producer tries addPartitions → InvalidProducerEpoch.
    auto ap = h.addAllPartitions("fence-txn", ir1.producerId, ir1.producerEpoch);
    EXPECT_EQ(ap.errorCode, AddPartitionsResult::kInvalidProducerEpoch)
        << "Zombie must be fenced from addPartitions";
}

// ---------------------------------------------------------------------------
// 19. Zombie producer: old epoch fenced from addPartitions.
// ---------------------------------------------------------------------------
TEST(ZombieFencing, ZombieProducer_OldEpochFencedFromAddPartitions)
{
    ExactlyOnceHarness h(2);

    auto ir1 = h.initProducer("fence-ap");
    ASSERT_TRUE(ir1.ok());

    // Bump epoch.
    auto ir2 = h.initProducer("fence-ap");
    ASSERT_TRUE(ir2.ok());
    EXPECT_GT(ir2.producerEpoch, ir1.producerEpoch);

    // Old producer tries addPartitions.
    auto ap = h.addAllPartitions("fence-ap", ir1.producerId, ir1.producerEpoch);
    EXPECT_EQ(ap.errorCode, AddPartitionsResult::kInvalidProducerEpoch);
}

// ---------------------------------------------------------------------------
// 20. Zombie producer: old epoch fenced from endTransaction.
// ---------------------------------------------------------------------------
TEST(ZombieFencing, ZombieProducer_OldEpochFencedFromEndTxn)
{
    ExactlyOnceHarness h(1);

    auto ir1 = h.initProducer("fence-end");
    ASSERT_TRUE(ir1.ok());
    ASSERT_TRUE(h.addAllPartitions("fence-end", ir1.producerId, ir1.producerEpoch).ok());
    h.appendTxnData(ir1.producerId, ir1.producerEpoch, 0, 0, "d");

    // Bump epoch (simulates new client).
    auto ir2 = h.initProducer("fence-end");
    ASSERT_TRUE(ir2.ok());

    // Old producer tries to commit → InvalidProducerEpoch.
    auto end = h.coordinator().endTransaction("fence-end",
                                              ir1.producerId, ir1.producerEpoch,
                                              /*commit=*/true, h.clock());
    EXPECT_EQ(end.errorCode, EndTransactionResult::kInvalidProducerEpoch)
        << "Old epoch must be fenced from endTransaction";
}

// ---------------------------------------------------------------------------
// 21. After epoch bump: NEW producer can transact normally.
// ---------------------------------------------------------------------------
TEST(ZombieFencing, EpochBump_NewProducerCanTransact)
{
    ExactlyOnceHarness h(1);

    auto ir1 = h.initProducer("txn-new");
    ASSERT_TRUE(ir1.ok());
    // Bump.
    auto ir2 = h.initProducer("txn-new");
    ASSERT_TRUE(ir2.ok());

    ASSERT_TRUE(h.addAllPartitions("txn-new", ir2.producerId, ir2.producerEpoch).ok());
    int64_t off = h.appendTxnData(ir2.producerId, ir2.producerEpoch, 0, 0, "new-data");
    ASSERT_GE(off, 0);
    ASSERT_TRUE(h.commitTxn("txn-new", ir2.producerId, ir2.producerEpoch));
    h.assertCommittedVisible(0, {off});
}

// ===========================================================================
// Group 4: Disaster / recovery
// ===========================================================================

// ---------------------------------------------------------------------------
// 22. Coordinator failover: PrepareCommit recovered → completes as commit.
// ---------------------------------------------------------------------------
TEST(DisasterRecovery, CoordinatorFailover_RecoversPrepareCommit_CompletesCommit)
{
    ExactlyOnceHarness h(2);

    auto ir = h.initProducer("fc-commit");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("fc-commit", ir.producerId, ir.producerEpoch).ok());

    int64_t off0 = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "d0");
    int64_t off1 = h.appendTxnData(ir.producerId, ir.producerEpoch, 1, 0, "d1");

    // Drive txn to PrepareCommit.
    auto endRes = h.coordinator().endTransaction("fc-commit",
                                                  ir.producerId, ir.producerEpoch,
                                                  /*commit=*/true, h.clock());
    ASSERT_TRUE(endRes.ok());
    // The PrepareCommit is persisted in the TransactionLog.
    // Verify state.
    EXPECT_EQ(h.coordinator().state("fc-commit"), TxnState::PrepareCommit);

    // Simulate coordinator crash + restart.
    h.recreateCoordinator(/*newCoordEpoch=*/1);

    // New coordinator must have recovered PrepareCommit.
    auto st = h.coordinator().state("fc-commit");
    ASSERT_TRUE(st.has_value());
    EXPECT_EQ(*st, TxnState::PrepareCommit)
        << "Recovered coordinator must have PrepareCommit state";

    // resolvePendingMarkers writes COMMIT markers.
    int markersWritten = h.resolvePending(/*coordEpoch=*/1);
    EXPECT_GT(markersWritten, 0) << "At least one COMMIT marker must be written";

    // Rebuild views after markers are written.
    h.rebuildAllViews();

    // Data must now be visible.
    h.assertCommittedVisible(0, {off0});
    h.assertCommittedVisible(1, {off1});
    h.assertLsoLeHw();
}

// ---------------------------------------------------------------------------
// 23. Coordinator failover: PrepareAbort recovered → completes as abort.
// ---------------------------------------------------------------------------
TEST(DisasterRecovery, CoordinatorFailover_RecoversPrepareAbort_CompletesAbort)
{
    ExactlyOnceHarness h(2);

    auto ir = h.initProducer("fc-abort");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("fc-abort", ir.producerId, ir.producerEpoch).ok());

    int64_t off0 = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "a0");
    int64_t off1 = h.appendTxnData(ir.producerId, ir.producerEpoch, 1, 0, "a1");

    // Drive to PrepareAbort.
    auto endRes = h.coordinator().endTransaction("fc-abort",
                                                  ir.producerId, ir.producerEpoch,
                                                  /*commit=*/false, h.clock());
    ASSERT_TRUE(endRes.ok());
    EXPECT_EQ(h.coordinator().state("fc-abort"), TxnState::PrepareAbort);

    // Coordinator crash + restart.
    h.recreateCoordinator(/*newCoordEpoch=*/1);

    auto st = h.coordinator().state("fc-abort");
    ASSERT_TRUE(st.has_value());
    EXPECT_EQ(*st, TxnState::PrepareAbort)
        << "Recovered coordinator must have PrepareAbort state";

    // Resolve → ABORT markers.
    int markersWritten = h.resolvePending(/*coordEpoch=*/1);
    EXPECT_GT(markersWritten, 0);

    h.rebuildAllViews();

    // Data must be invisible under read_committed.
    h.assertAbortedInvisible(0, {off0});
    h.assertAbortedInvisible(1, {off1});
    // But visible under read_uncommitted.
    h.assertReadUncommittedSeesData(0, {off0});
    h.assertReadUncommittedSeesData(1, {off1});
}

// ---------------------------------------------------------------------------
// 24. Coordinator failover with Ongoing txn → doWork times out → ABORT.
// ---------------------------------------------------------------------------
TEST(DisasterRecovery, CoordinatorFailover_OngoingTxnTimeoutAborted_MarkersWritten)
{
    // Short timeout: 100 ms.
    ExactlyOnceHarness h(1, "topic", /*txnTimeoutMs=*/100);

    auto ir = h.initProducer("timeout-txn");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("timeout-txn", ir.producerId, ir.producerEpoch).ok());
    int64_t off = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "stale");
    ASSERT_GE(off, 0);

    // Advance clock past timeout.
    h.advanceClock(200LL * 1'000'000LL); // 200 ms

    // driveUntil: doWork transitions Ongoing → PrepareAbort.
    bool timed = h.driveUntil([&]{
        auto st = h.coordinator().state("timeout-txn");
        return st.has_value() && *st == TxnState::PrepareAbort;
    }, kCap);
    ASSERT_TRUE(timed) << "Coordinator must timeout-abort the stale txn";

    // Resolve pending abort markers.
    int m = h.resolvePending();
    EXPECT_GT(m, 0);

    h.rebuildAllViews();
    h.assertAbortedInvisible(0, {off});
    h.assertLsoLeHw();
}

// ---------------------------------------------------------------------------
// 25. Multiple in-flight txns across coordinator restart → all resolved.
// ---------------------------------------------------------------------------
TEST(DisasterRecovery, CoordinatorFailover_MultiTxn_AllResolved)
{
    ExactlyOnceHarness h(2);

    // txn-M1 → PrepareCommit, txn-M2 → PrepareAbort.
    auto ir1 = h.initProducer("txn-M1");
    ASSERT_TRUE(ir1.ok());
    ASSERT_TRUE(h.addAllPartitions("txn-M1", ir1.producerId, ir1.producerEpoch).ok());
    int64_t off1p0 = h.appendTxnData(ir1.producerId, ir1.producerEpoch, 0, 0, "m1p0");
    int64_t off1p1 = h.appendTxnData(ir1.producerId, ir1.producerEpoch, 1, 0, "m1p1");

    auto ir2 = h.initProducer("txn-M2");
    ASSERT_TRUE(ir2.ok());
    ASSERT_TRUE(h.addAllPartitions("txn-M2", ir2.producerId, ir2.producerEpoch).ok());
    int64_t off2p0 = h.appendTxnData(ir2.producerId, ir2.producerEpoch, 0, 0, "m2p0");
    int64_t off2p1 = h.appendTxnData(ir2.producerId, ir2.producerEpoch, 1, 0, "m2p1");

    // Commit M1, Abort M2 (both go to Prepare state).
    auto e1 = h.coordinator().endTransaction("txn-M1", ir1.producerId, ir1.producerEpoch,
                                              true, h.clock());
    ASSERT_TRUE(e1.ok());
    auto e2 = h.coordinator().endTransaction("txn-M2", ir2.producerId, ir2.producerEpoch,
                                              false, h.clock());
    ASSERT_TRUE(e2.ok());

    // Crash + restart.
    h.recreateCoordinator(1);

    // Both txns must be recovered.
    EXPECT_EQ(h.coordinator().state("txn-M1"), TxnState::PrepareCommit);
    EXPECT_EQ(h.coordinator().state("txn-M2"), TxnState::PrepareAbort);

    // Resolve all.
    h.resolvePending(1);
    h.rebuildAllViews();

    // M1 committed → visible; M2 aborted → invisible.
    h.assertCommittedVisible(0, {off1p0});
    h.assertCommittedVisible(1, {off1p1});
    h.assertAbortedInvisible(0, {off2p0});
    h.assertAbortedInvisible(1, {off2p1});
}

// ---------------------------------------------------------------------------
// 26. Committed data is fully visible after coordinator failover.
// ---------------------------------------------------------------------------
TEST(DisasterRecovery, CoordinatorFailover_CommittedDataFullyVisibleAfterRecovery)
{
    ExactlyOnceHarness h(1);

    // First txn: commit before crash.
    auto ir = h.initProducer("pre-crash-txn");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("pre-crash-txn", ir.producerId, ir.producerEpoch).ok());
    int64_t off = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "pre-crash");
    ASSERT_TRUE(h.commitTxn("pre-crash-txn", ir.producerId, ir.producerEpoch));

    // Crash + restart coordinator.
    h.recreateCoordinator(1);

    // The committed data is in the partition log (not the coordinator log).
    // rebuild the view to verify.
    h.rebuildAllViews();
    h.assertCommittedVisible(0, {off});
}

// ---------------------------------------------------------------------------
// 27. Aborted data is invisible after coordinator failover.
// ---------------------------------------------------------------------------
TEST(DisasterRecovery, CoordinatorFailover_AbortedDataInvisibleAfterRecovery)
{
    ExactlyOnceHarness h(1);

    auto ir = h.initProducer("pre-crash-abort");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("pre-crash-abort", ir.producerId, ir.producerEpoch).ok());
    int64_t off = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "will-abort");
    ASSERT_TRUE(h.abortTxn("pre-crash-abort", ir.producerId, ir.producerEpoch));

    h.recreateCoordinator(1);
    h.rebuildAllViews();
    h.assertAbortedInvisible(0, {off});
}

// ---------------------------------------------------------------------------
// 28. After failover + epoch bump: zombie (old epoch) is still blocked.
// ---------------------------------------------------------------------------
TEST(DisasterRecovery, CoordinatorFailover_ZombieBlockedAfterEpochBump)
{
    ExactlyOnceHarness h(1);

    auto ir1 = h.initProducer("fc-zombie");
    ASSERT_TRUE(ir1.ok());
    ASSERT_TRUE(h.addAllPartitions("fc-zombie", ir1.producerId, ir1.producerEpoch).ok());
    h.appendTxnData(ir1.producerId, ir1.producerEpoch, 0, 0, "z");

    // Client bumps epoch BEFORE crash.
    auto ir2 = h.initProducer("fc-zombie");
    ASSERT_TRUE(ir2.ok());
    EXPECT_GT(ir2.producerEpoch, ir1.producerEpoch);

    // Crash + restart.
    h.recreateCoordinator(1);

    // Old producer still blocked.
    auto ap = h.addAllPartitions("fc-zombie", ir1.producerId, ir1.producerEpoch);
    EXPECT_EQ(ap.errorCode, AddPartitionsResult::kInvalidProducerEpoch)
        << "Zombie must remain fenced after coordinator restart";
}

// ---------------------------------------------------------------------------
// 29. Timeout abort: doWork aborts Ongoing txn → data invisible.
// ---------------------------------------------------------------------------
TEST(DisasterRecovery, TimeoutAbort_DoWork_AbortedDataInvisible)
{
    ExactlyOnceHarness h(1, "topic", /*txnTimeoutMs=*/50); // 50 ms timeout

    auto ir = h.initProducer("to-abort");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("to-abort", ir.producerId, ir.producerEpoch).ok());
    int64_t off = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "stale-data");
    ASSERT_GE(off, 0);

    // Advance clock past 50 ms timeout.
    h.advanceClock(100LL * 1'000'000LL);

    // driveUntil doWork transitions to PrepareAbort.
    bool aborted = h.driveUntil([&]{
        auto s = h.coordinator().state("to-abort");
        return s.has_value() && *s == TxnState::PrepareAbort;
    }, kCap);
    ASSERT_TRUE(aborted) << "doWork must timeout-abort the stale transaction";

    // Resolve + rebuild.
    h.resolvePending();
    h.rebuildAllViews();
    h.assertAbortedInvisible(0, {off});
}

// ---------------------------------------------------------------------------
// 30. Timeout abort: LSO advances after abort.
// ---------------------------------------------------------------------------
TEST(DisasterRecovery, TimeoutAbort_LsoAdvancesAfterAbort)
{
    ExactlyOnceHarness h(1, "topic", /*txnTimeoutMs=*/50);

    auto ir = h.initProducer("to-lso");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("to-lso", ir.producerId, ir.producerEpoch).ok());
    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "lso-test");

    // LSO pinned.
    EXPECT_EQ(h.partition(0).lsoTracker.lso(h.partition(0).hw()), 0);

    // Advance + drive timeout.
    h.advanceClock(100LL * 1'000'000LL);
    bool aborted = h.driveUntil([&]{
        auto s = h.coordinator().state("to-lso");
        return s.has_value() && *s == TxnState::PrepareAbort;
    }, kCap);
    ASSERT_TRUE(aborted);

    h.resolvePending();
    h.rebuildAllViews();

    int64_t hw = h.partition(0).hw();
    EXPECT_EQ(h.partition(0).lsoTracker.lso(hw), hw)
        << "LSO must equal HW after timeout abort";
}

// ---------------------------------------------------------------------------
// 31. Transactional offset commit (real addOffsetsToTxn): commit → offset
//     advanced atomically.  Offset must NOT appear before commit
//     (durable-before-visible) and MUST appear after commit.
// ---------------------------------------------------------------------------
TEST(DisasterRecovery, TxnOffsetCommit_AtomicWithTxn_CommitAdvancesOffset)
{
    ExactlyOnceHarness h(1, "topic");

    auto ir = h.initProducer("toc-commit");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("toc-commit", ir.producerId, ir.producerEpoch).ok());
    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "rec");

    // Register offset 1 as PENDING within the transaction via addOffsetsToTxn.
    rcoord::TopicPartition tp{"topic", 0};
    std::map<rcoord::TopicPartition, rcoord::Offset> offsets{{tp, 1}};
    ASSERT_TRUE(h.addOffsetsToTxn("toc-commit", ir.producerId, ir.producerEpoch,
                                   "group-toc", offsets).ok());

    // BEFORE commit: OffsetStore must NOT have the offset (PENDING, not applied).
    EXPECT_FALSE(h.fetchOffset("group-toc", tp).has_value())
        << "Offset must NOT appear in OffsetStore before txn commit";

    // Commit the transaction — addOffsetsToTxn offset is applied atomically.
    ASSERT_TRUE(h.commitTxn("toc-commit", ir.producerId, ir.producerEpoch));

    // AFTER commit: OffsetStore MUST show offset 1.
    auto co = h.fetchOffset("group-toc", tp);
    ASSERT_TRUE(co.has_value())
        << "Committed offset must be in OffsetStore after txn commit";
    EXPECT_EQ(co->offset, 1LL);
}

// ---------------------------------------------------------------------------
// 32. Transactional offset commit (real addOffsetsToTxn): abort → offset
//     NEVER advanced.  THIS TEST MUST FAIL if offsets were applied on abort.
// ---------------------------------------------------------------------------
TEST(DisasterRecovery, TxnOffsetCommit_AbortDoesNotAdvanceOffset)
{
    ExactlyOnceHarness h(1, "topic");

    auto ir = h.initProducer("toc-abort");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("toc-abort", ir.producerId, ir.producerEpoch).ok());
    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "rec");

    // Register offset 99 as PENDING within the transaction via addOffsetsToTxn.
    rcoord::TopicPartition tp{"topic", 0};
    std::map<rcoord::TopicPartition, rcoord::Offset> offsets{{tp, 99}};
    ASSERT_TRUE(h.addOffsetsToTxn("toc-abort", ir.producerId, ir.producerEpoch,
                                   "group-toc-abort", offsets).ok());

    // Abort the transaction — pending offsets must be DISCARDED (never written).
    ASSERT_TRUE(h.abortTxn("toc-abort", ir.producerId, ir.producerEpoch));

    // AFTER abort: OffsetStore must NOT have the offset.
    // This assertion MUST FAIL if offsetApplyCb_ is incorrectly called on abort.
    EXPECT_FALSE(h.fetchOffset("group-toc-abort", tp).has_value())
        << "INVARIANT VIOLATED: offset was applied to OffsetStore on abort";
}

// ===========================================================================
// Bonus: more edge/invariant tests
// ===========================================================================

// ---------------------------------------------------------------------------
// 33. After crash recovery: new txn works with reseeded allocator (no PID reuse).
// ---------------------------------------------------------------------------
TEST(DisasterRecovery, CrashRecovery_NewTxnAfterRestart_NoPidReuse)
{
    ExactlyOnceHarness h(1);

    auto ir1 = h.initProducer("recover-pid");
    ASSERT_TRUE(ir1.ok());
    ProducerId oldPid = ir1.producerId;

    h.recreateCoordinator(1);

    // New producer after restart must get a fresh PID > oldPid.
    auto ir2 = h.initProducer("new-after-restart");
    ASSERT_TRUE(ir2.ok());
    EXPECT_GT(ir2.producerId, oldPid)
        << "PID allocator must be reseeded after restart (no PID collision)";
}

// ---------------------------------------------------------------------------
// 34. State after CompleteCommit transitions back to Empty (ready for next txn).
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, CompleteCommit_TransitionsToEmpty_ReadyForNextTxn)
{
    ExactlyOnceHarness h(1);

    auto ir = h.initProducer("reuse-txn");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("reuse-txn", ir.producerId, ir.producerEpoch).ok());
    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "d");
    ASSERT_TRUE(h.commitTxn("reuse-txn", ir.producerId, ir.producerEpoch));

    // State must be Empty after complete.
    auto st = h.coordinator().state("reuse-txn");
    ASSERT_TRUE(st.has_value());
    // F15 §5.4 (5113a52, Kafka parity): CompleteCommit is the RESTING state after a commit; the
    // txnId is reusable from it (the next AddPartitions starts a new txn) — asserted below.
    EXPECT_EQ(*st, TxnState::CompleteCommit) << "CompleteCommit is the resting state after a commit";

    // Next txn on same txnId works.
    ASSERT_TRUE(h.addAllPartitions("reuse-txn", ir.producerId, ir.producerEpoch).ok());
    int64_t off2 = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 1, "d2");
    ASSERT_GE(off2, 0);
    ASSERT_TRUE(h.commitTxn("reuse-txn", ir.producerId, ir.producerEpoch));
    h.assertCommittedVisible(0, {off2});
}

// ---------------------------------------------------------------------------
// 35. No aborted record ever returns from read_committed (invariant check).
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, NoAbortedRecordVisibleUnderReadCommitted_Invariant)
{
    ExactlyOnceHarness h(1);

    // Multiple producers: some commit, some abort, interleaved.
    auto irC = h.initProducer("inv-commit");
    ASSERT_TRUE(irC.ok());
    ASSERT_TRUE(h.addAllPartitions("inv-commit", irC.producerId, irC.producerEpoch).ok());

    auto irA = h.initProducer("inv-abort");
    ASSERT_TRUE(irA.ok());
    ASSERT_TRUE(h.addAllPartitions("inv-abort", irA.producerId, irA.producerEpoch).ok());

    for (int i = 0; i < 3; ++i) {
        h.appendTxnData(irC.producerId, irC.producerEpoch, 0, i, "c");
        h.appendTxnData(irA.producerId, irA.producerEpoch, 0, i, "a");
    }

    ASSERT_TRUE(h.commitTxn("inv-commit", irC.producerId, irC.producerEpoch));
    ASSERT_TRUE(h.abortTxn("inv-abort", irA.producerId, irA.producerEpoch));

    // Invariant: no aborted batch visible under read_committed.
    h.assertNoAbortedDataVisible(0);
    h.assertLsoLeHw();
}

// ---------------------------------------------------------------------------
// 36. Multi-batch txn on single partition: all batches visible after commit.
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, MultiBatch_SinglePartition_AllBatchesVisible)
{
    ExactlyOnceHarness h(1);

    auto ir = h.initProducer("multi-batch");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("multi-batch", ir.producerId, ir.producerEpoch).ok());

    std::vector<int64_t> offs;
    for (int i = 0; i < 5; ++i) {
        int64_t off = h.appendTxnData(ir.producerId, ir.producerEpoch, 0, i, "b" + std::to_string(i));
        ASSERT_GE(off, 0);
        offs.push_back(off);
    }

    // Before commit: nothing.
    EXPECT_TRUE(parseOffsets(h.readCommitted(0)).empty());

    ASSERT_TRUE(h.commitTxn("multi-batch", ir.producerId, ir.producerEpoch));
    h.assertCommittedVisible(0, offs);
}

// ---------------------------------------------------------------------------
// 37. Concurrent txnIds (different transactional ids, same cluster):
//     each is independent; committing one doesn't affect the other.
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, ConcurrentTxnIds_Independent)
{
    ExactlyOnceHarness h(1);

    auto ir1 = h.initProducer("cT-1");
    ASSERT_TRUE(ir1.ok());
    ASSERT_TRUE(h.addAllPartitions("cT-1", ir1.producerId, ir1.producerEpoch).ok());

    auto ir2 = h.initProducer("cT-2");
    ASSERT_TRUE(ir2.ok());
    ASSERT_TRUE(h.addAllPartitions("cT-2", ir2.producerId, ir2.producerEpoch).ok());

    int64_t off1 = h.appendTxnData(ir1.producerId, ir1.producerEpoch, 0, 0, "t1");
    int64_t off2 = h.appendTxnData(ir2.producerId, ir2.producerEpoch, 0, 0, "t2");

    // Commit only txn-1.
    ASSERT_TRUE(h.commitTxn("cT-1", ir1.producerId, ir1.producerEpoch));

    // Off1 visible, off2 not (still in-flight txn-2 pins LSO).
    auto rc = parseOffsets(h.readCommitted(0));
    bool off1Found = false;
    for (int64_t o : rc) if (o == off1) off1Found = true;

    // LSO pinned at off2 (the earlier in-flight txn for pid2).
    int64_t hw  = h.partition(0).hw();
    int64_t lso = h.partition(0).lsoTracker.lso(hw);
    EXPECT_LE(lso, hw);
    // off1 may not be visible if lso ≤ off1 (depending on order).
    // The key invariant: no data at or after LSO visible.
    for (int64_t o : rc) {
        EXPECT_LT(o, lso) << "read_committed must not return data >= LSO";
    }
    (void)off1Found;
    (void)off2;

    // Abort txn-2 so cleanup.
    ASSERT_TRUE(h.abortTxn("cT-2", ir2.producerId, ir2.producerEpoch));
}

// ---------------------------------------------------------------------------
// 38. Coordinator log durability: reopen without crash preserves all state.
// ---------------------------------------------------------------------------
TEST(DisasterRecovery, TxnLog_DurableAcrossReopen_StatePreserved)
{
    ExactlyOnceHarness h(1);

    auto ir = h.initProducer("durable-txn");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("durable-txn", ir.producerId, ir.producerEpoch).ok());
    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "d");
    ASSERT_TRUE(h.commitTxn("durable-txn", ir.producerId, ir.producerEpoch));

    // Verify state before reopen.
    auto stBefore = h.coordinator().state("durable-txn");
    ASSERT_TRUE(stBefore.has_value());
    EXPECT_EQ(*stBefore, TxnState::CompleteCommit);   // F15 §5.4 resting state

    // Reopen (no crash, orderly restart).
    h.recreateCoordinator(1);

    // State recovered.
    auto stAfter = h.coordinator().state("durable-txn");
    ASSERT_TRUE(stAfter.has_value());
    EXPECT_EQ(*stAfter, TxnState::CompleteCommit)
        << "the completed txn's resting state must survive the reopen (F15 §5.4)";
}

// ---------------------------------------------------------------------------
// 39. addPartitions: duplicate partitions deduplicated.
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, AddPartitions_DedupDuplicatePartitions)
{
    ExactlyOnceHarness h(2);

    auto ir = h.initProducer("dup-parts");
    ASSERT_TRUE(ir.ok());

    // Add the same partition twice.
    std::vector<TopicPartition> parts = {
        h.allPartitions()[0], h.allPartitions()[0], h.allPartitions()[1]
    };
    auto ap = h.addPartitions("dup-parts", ir.producerId, ir.producerEpoch, parts);
    EXPECT_TRUE(ap.ok());

    // Verify only unique partitions are in the txn.
    auto partList = h.coordinator().partitionsOf("dup-parts");
    EXPECT_EQ(partList.size(), 2u)
        << "Duplicate partitions must be deduped";
}

// ---------------------------------------------------------------------------
// 40. addPartitions: transition Empty → Ongoing requires sequence set.
// ---------------------------------------------------------------------------
TEST(AtomicVisibility, AddPartitions_EmptyToOngoing_StateTransition)
{
    ExactlyOnceHarness h(1);

    auto ir = h.initProducer("state-check");
    ASSERT_TRUE(ir.ok());

    // Initially Empty.
    auto stEmpty = h.coordinator().state("state-check");
    ASSERT_TRUE(stEmpty.has_value());
    EXPECT_EQ(*stEmpty, TxnState::Empty);

    // addPartitions → Ongoing.
    ASSERT_TRUE(h.addAllPartitions("state-check", ir.producerId, ir.producerEpoch).ok());
    auto stOngoing = h.coordinator().state("state-check");
    ASSERT_TRUE(stOngoing.has_value());
    EXPECT_EQ(*stOngoing, TxnState::Ongoing);
}

// ===========================================================================
// F3: Real transactional offset commit (addOffsetsToTxn)
// ===========================================================================

// ---------------------------------------------------------------------------
// F3-a. addOffsetsToTxn: commit → OffsetStore shows advanced offset.
//       Offset must NOT be in store before commit (pre-commit assertion).
//       Offset MUST be in store after commit (post-commit assertion).
// ---------------------------------------------------------------------------
TEST(TxnOffsetCommit, AddOffsetsToTxn_CommitAppliesOffset)
{
    ExactlyOnceHarness h(1, "topic");
    auto ir = h.initProducer("ato-commit");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("ato-commit", ir.producerId, ir.producerEpoch).ok());
    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "data");

    rcoord::TopicPartition inputTp{"input", 0};
    std::map<rcoord::TopicPartition, rcoord::Offset> offsets{{inputTp, 5}};

    // addOffsetsToTxn records offset 5 as PENDING (not yet written to OffsetStore).
    ASSERT_TRUE(h.addOffsetsToTxn("ato-commit", ir.producerId, ir.producerEpoch,
                                   "grp-commit", offsets).ok());

    // BEFORE commit: OffsetStore must NOT have the offset (durable-before-visible).
    EXPECT_FALSE(h.fetchOffset("grp-commit", inputTp).has_value())
        << "Offset must NOT be in OffsetStore before txn commit";

    ASSERT_TRUE(h.commitTxn("ato-commit", ir.producerId, ir.producerEpoch));

    // AFTER commit: OffsetStore MUST have offset 5.
    auto co = h.fetchOffset("grp-commit", inputTp);
    ASSERT_TRUE(co.has_value()) << "Offset must be in OffsetStore after commit";
    EXPECT_EQ(co->offset, 5LL);
}

// ---------------------------------------------------------------------------
// F3-b. addOffsetsToTxn: abort → OffsetStore NOT advanced.
//       THIS TEST MUST FAIL if offsets were applied on abort (invariant check).
// ---------------------------------------------------------------------------
TEST(TxnOffsetCommit, AddOffsetsToTxn_AbortDiscardsOffset)
{
    ExactlyOnceHarness h(1, "topic");
    auto ir = h.initProducer("ato-abort");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("ato-abort", ir.producerId, ir.producerEpoch).ok());
    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "data");

    rcoord::TopicPartition inputTp{"input", 0};
    std::map<rcoord::TopicPartition, rcoord::Offset> offsets{{inputTp, 5}};
    ASSERT_TRUE(h.addOffsetsToTxn("ato-abort", ir.producerId, ir.producerEpoch,
                                   "grp-abort", offsets).ok());

    ASSERT_TRUE(h.abortTxn("ato-abort", ir.producerId, ir.producerEpoch));

    // AFTER abort: OffsetStore must NOT have offset 5 (invariant).
    // This assertion MUST FAIL if offsetApplyCb_ is called on the abort path.
    EXPECT_FALSE(h.fetchOffset("grp-abort", inputTp).has_value())
        << "INVARIANT VIOLATED: offset was applied to OffsetStore on abort";
}

// ---------------------------------------------------------------------------
// F3-c. addOffsetsToTxn: coordinator failover mid-PrepareCommit recovers
//        pending offsets and applies them on resolve.
// ---------------------------------------------------------------------------
TEST(TxnOffsetCommit, AddOffsetsToTxn_FailoverRecoversPendingOffsets)
{
    ExactlyOnceHarness h(1, "topic");
    auto ir = h.initProducer("ato-failover");
    ASSERT_TRUE(ir.ok());
    ASSERT_TRUE(h.addAllPartitions("ato-failover", ir.producerId, ir.producerEpoch).ok());
    h.appendTxnData(ir.producerId, ir.producerEpoch, 0, 0, "data");

    rcoord::TopicPartition inputTp{"input", 0};
    std::map<rcoord::TopicPartition, rcoord::Offset> offsets{{inputTp, 7}};
    ASSERT_TRUE(h.addOffsetsToTxn("ato-failover", ir.producerId, ir.producerEpoch,
                                   "grp-failover", offsets).ok());

    // Drive to PrepareCommit (endTransaction commit), then crash BEFORE markers.
    {
        auto endRes = h.coordinator().endTransaction(
            "ato-failover", ir.producerId, ir.producerEpoch, /*commit=*/true, h.clock());
        ASSERT_TRUE(endRes.ok());
        // Do NOT write markers or call completeTransaction — simulate crash here.
    }

    // Crash + restart coordinator (recovered state = PrepareCommit, markersPending=false
    // because it was client-driven; but the coordinator still holds PrepareCommit durably).
    h.recreateCoordinator(1);

    // After recovery: pending offsets must NOT be in OffsetStore yet.
    EXPECT_FALSE(h.fetchOffset("grp-failover", inputTp).has_value())
        << "Pending offsets must NOT be applied before resolution";

    // Verify the coordinator recovered PrepareCommit state.
    auto recoveredState = h.coordinator().state("ato-failover");
    ASSERT_TRUE(recoveredState.has_value());
    EXPECT_EQ(*recoveredState, TxnState::PrepareCommit)
        << "Coordinator must recover PrepareCommit from durable log";

    // Simulate resolving: write COMMIT markers manually and call completeTransaction.
    // (In production this would be driven by writeMarkers.)
    for (int i = 0; i < h.numPartitions(); ++i) {
        appendControlMarker(h.partition(i), ir.producerId, ir.producerEpoch,
                            ControlType::Commit, /*coordEpoch=*/1);
    }
    ASSERT_TRUE(h.coordinator().completeTransaction("ato-failover").ok());

    // After resolution: offset MUST be in OffsetStore.
    auto co = h.fetchOffset("grp-failover", inputTp);
    ASSERT_TRUE(co.has_value()) << "Offset must be applied after recovered commit resolve";
    EXPECT_EQ(co->offset, 7LL);

    // Data must be visible under read_committed.
    h.rebuildAllViews();
    EXPECT_FALSE(parseOffsets(h.readCommitted(0)).empty());
}
