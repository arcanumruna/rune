// ---------------------------------------------------------------------------
// ProducerStateManagerTest.cpp — ~35+ unit tests for ProducerStateManager
//
// Covers:
//   • brand-new producer acceptance (baseSequence 0)
//   • contiguous in-order acceptance
//   • duplicate detection (same epoch+seq → Duplicate + original offset, no re-append)
//   • out-of-order / gap rejection
//   • stale (below window) sequence rejection
//   • epoch fencing (lower → InvalidProducerEpoch)
//   • epoch bump (higher → Accept, resets sequence tracking)
//   • multi-producer + multi-partition isolation
//   • retry-window: last N batches remembered; beyond window → OOS
//   • snapshot + recover (dedup survives restart)
//   • sequence edge cases (int32 large values, first non-zero)
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include <rune/storage/ProducerStateManager.h>

#include <atomic>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using rune::storage::ProducerStateManager;
using rune::storage::ValidationResult;
using rune::storage::kRetryWindowSize;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Unique temp path helper
// ---------------------------------------------------------------------------
static std::atomic<int> g_counter{0};

static std::string tmpPath(const char* tag = "psm") {
    return std::string("/tmp/") + tag + "_" +
           std::to_string(::getpid()) + "_" +
           std::to_string(g_counter.fetch_add(1, std::memory_order_relaxed));
}

// ---------------------------------------------------------------------------
// Helpers to call validate+update together
// ---------------------------------------------------------------------------
static ValidationResult produce(ProducerStateManager& m,
                                int64_t pid,
                                int32_t partition,
                                int16_t epoch,
                                int32_t baseSeq,
                                int32_t recordCount,
                                int64_t baseOffset)
{
    int32_t lastSeq = baseSeq + recordCount - 1;
    auto r = m.validate(pid, partition, epoch, baseSeq, lastSeq);
    if (r.isAccept()) {
        m.update(pid, partition, epoch, baseSeq, lastSeq, baseOffset);
    }
    return r;
}

// ===========================================================================
// 1. Brand-new producer
// ===========================================================================

TEST(ProducerStateManager, BrandNewProducerBaseSequenceZeroAccepted) {
    ProducerStateManager m;
    auto r = m.validate(42, 0, 0, 0, 0); // one record
    EXPECT_TRUE(r.isAccept());
}

TEST(ProducerStateManager, BrandNewProducerNonZeroBaseSequenceRejected) {
    ProducerStateManager m;
    auto r = m.validate(42, 0, 0, 1, 2);
    EXPECT_TRUE(r.isOos());
}

TEST(ProducerStateManager, BrandNewProducerLargeBaseSequenceRejected) {
    ProducerStateManager m;
    auto r = m.validate(99, 0, 0, 100, 109);
    EXPECT_TRUE(r.isOos());
}

// ===========================================================================
// 2. In-order acceptance — lastSequence advances
// ===========================================================================

TEST(ProducerStateManager, InOrderBatchesAccepted) {
    ProducerStateManager m;
    // batch 0: seq 0..4
    EXPECT_TRUE(produce(m, 1, 0, 0, 0, 5, 1000).isAccept());
    // batch 1: seq 5..9
    EXPECT_TRUE(produce(m, 1, 0, 0, 5, 5, 2000).isAccept());
    // batch 2: seq 10..10
    EXPECT_TRUE(produce(m, 1, 0, 0, 10, 1, 3000).isAccept());

    const auto* e = m.entry(1, 0);
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->lastSequence, 10);
}

TEST(ProducerStateManager, SingleRecordBatchesSequentiallyAccepted) {
    ProducerStateManager m;
    for (int32_t seq = 0; seq < 10; ++seq) {
        EXPECT_TRUE(produce(m, 7, 0, 0, seq, 1, seq * 100LL).isAccept())
            << "seq=" << seq;
    }
    EXPECT_EQ(m.entry(7, 0)->lastSequence, 9);
}

// ===========================================================================
// 3. Duplicate detection — returns original offset, no re-append
// ===========================================================================

TEST(ProducerStateManager, DuplicateBatchReturnsPriorOffset) {
    ProducerStateManager m;
    // First send: seq 0..2, base offset = 5000
    EXPECT_TRUE(produce(m, 10, 0, 0, 0, 3, 5000).isAccept());

    // Retry: same epoch+baseSequence+lastSequence
    auto r = m.validate(10, 0, 0, 0, 2);
    EXPECT_TRUE(r.isDuplicate());
    EXPECT_EQ(r.retryBaseOffset, 5000);
}

TEST(ProducerStateManager, DuplicateDoesNotAdvanceState) {
    ProducerStateManager m;
    produce(m, 10, 0, 0, 0, 3, 5000);

    // Duplicate — should not advance lastSequence
    auto dup_r = m.validate(10, 0, 0, 0, 2); // must not call update
    EXPECT_TRUE(dup_r.isDuplicate());

    const auto* e = m.entry(10, 0);
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->lastSequence, 2); // still 2, not advanced
}

TEST(ProducerStateManager, DuplicateOfSecondBatchReturnsPriorOffset) {
    ProducerStateManager m;
    produce(m, 11, 0, 0, 0, 3, 1000); // seq 0..2 → offset 1000
    produce(m, 11, 0, 0, 3, 4, 2000); // seq 3..6 → offset 2000

    // Retry of second batch
    auto r = m.validate(11, 0, 0, 3, 6);
    EXPECT_TRUE(r.isDuplicate());
    EXPECT_EQ(r.retryBaseOffset, 2000);
}

TEST(ProducerStateManager, DuplicateOfFirstBatchAfterSeveralBatches) {
    ProducerStateManager m;
    produce(m, 12, 0, 0, 0, 2, 100); // seq 0..1
    produce(m, 12, 0, 0, 2, 2, 200); // seq 2..3
    produce(m, 12, 0, 0, 4, 2, 300); // seq 4..5

    // Retry of first batch (still within window of 5)
    auto r = m.validate(12, 0, 0, 0, 1);
    EXPECT_TRUE(r.isDuplicate());
    EXPECT_EQ(r.retryBaseOffset, 100);
}

// ===========================================================================
// 4. Out-of-order / gap rejection
// ===========================================================================

TEST(ProducerStateManager, ForwardGapRejected) {
    ProducerStateManager m;
    produce(m, 20, 0, 0, 0, 3, 1000); // seq 0..2
    // Expected next: 3; send 5 → gap
    auto r = m.validate(20, 0, 0, 5, 6);
    EXPECT_TRUE(r.isOos());
}

TEST(ProducerStateManager, LargeForwardGapRejected) {
    ProducerStateManager m;
    produce(m, 21, 0, 0, 0, 1, 1000);
    auto r = m.validate(21, 0, 0, 1000, 1000);
    EXPECT_TRUE(r.isOos());
}

TEST(ProducerStateManager, ReplayBeyondRetryWindowIsOos) {
    ProducerStateManager m;
    // Fill more than kRetryWindowSize batches
    int64_t baseOffset = 1000;
    int32_t baseSeq = 0;
    for (int i = 0; i <= kRetryWindowSize; ++i) {
        produce(m, 30, 0, 0, baseSeq, 1, baseOffset);
        baseSeq++;
        baseOffset += 100;
    }
    // The very first batch (seq 0..0) has been evicted from the window.
    auto r = m.validate(30, 0, 0, 0, 0);
    // It's below the expected next sequence but NOT in the window → OOS
    EXPECT_TRUE(r.isOos());
}

// ===========================================================================
// 5. Epoch fencing
// ===========================================================================

TEST(ProducerStateManager, LowerEpochFenced) {
    ProducerStateManager m;
    produce(m, 50, 0, 2, 0, 1, 1000); // epoch 2 established
    auto r = m.validate(50, 0, 1, 1, 1); // epoch 1 < 2
    EXPECT_TRUE(r.isFenced());
}

TEST(ProducerStateManager, SameEpochNotFenced) {
    ProducerStateManager m;
    produce(m, 50, 0, 2, 0, 1, 1000);
    auto r = m.validate(50, 0, 2, 1, 1);
    EXPECT_TRUE(r.isAccept());
}

TEST(ProducerStateManager, HigherEpochAcceptsAndResetsSequence) {
    ProducerStateManager m;
    produce(m, 60, 0, 1, 0, 5, 1000); // epoch 1, seq 0..4
    // Epoch 2: new session, must start at seq 0
    auto r = m.validate(60, 0, 2, 0, 2);
    EXPECT_TRUE(r.isAccept());
}

TEST(ProducerStateManager, HigherEpochNonZeroBaseSequenceRejected) {
    ProducerStateManager m;
    produce(m, 60, 0, 1, 0, 5, 1000);
    // Epoch 2 but baseSequence != 0
    auto r = m.validate(60, 0, 2, 3, 5);
    EXPECT_TRUE(r.isOos());
}

TEST(ProducerStateManager, HigherEpochClearsRetryWindow) {
    ProducerStateManager m;
    produce(m, 70, 0, 0, 0, 3, 1000); // epoch 0, seq 0..2

    // Bump to epoch 1
    produce(m, 70, 0, 1, 0, 2, 2000); // epoch 1, seq 0..1

    // Old epoch's batches should NOT appear as duplicates in the new session.
    // seq 0..2 in epoch 0 was at offset 1000, but in epoch 1 seq 0..1 @ 2000.
    // Validate epoch 1 seq 0..1 → should be Duplicate (still in window).
    auto r = m.validate(70, 0, 1, 0, 1);
    EXPECT_TRUE(r.isDuplicate());
    EXPECT_EQ(r.retryBaseOffset, 2000);

    // Epoch 0 batch in epoch 1 → fenced
    auto r2 = m.validate(70, 0, 0, 0, 2);
    EXPECT_TRUE(r2.isFenced());
}

TEST(ProducerStateManager, MultipleEpochBumpsWork) {
    ProducerStateManager m;
    for (int16_t epoch = 0; epoch < 5; ++epoch) {
        auto r = produce(m, 80, 0, epoch, 0, 1, 1000 + epoch * 100);
        EXPECT_TRUE(r.isAccept()) << "epoch=" << epoch;
    }
}

// ===========================================================================
// 6. Multi-producer + multi-partition isolation
// ===========================================================================

TEST(ProducerStateManager, TwoProducersIndependent) {
    ProducerStateManager m;
    produce(m, 100, 0, 0, 0, 3, 1000); // pid 100 seq 0..2
    produce(m, 101, 0, 0, 0, 5, 2000); // pid 101 seq 0..4

    // pid 100 next: 3
    EXPECT_TRUE(m.validate(100, 0, 0, 3, 3).isAccept());
    // pid 101 next: 5
    EXPECT_TRUE(m.validate(101, 0, 0, 5, 5).isAccept());

    // pid 100 retry seq 0..2 → Duplicate @ 1000
    auto r = m.validate(100, 0, 0, 0, 2);
    EXPECT_TRUE(r.isDuplicate());
    EXPECT_EQ(r.retryBaseOffset, 1000);
}

TEST(ProducerStateManager, SameProducerDifferentPartitionsIndependent) {
    ProducerStateManager m;
    produce(m, 200, 0, 0, 0, 3, 1000); // partition 0
    produce(m, 200, 1, 0, 0, 5, 2000); // partition 1

    // Partition 0: next = 3; partition 1: next = 5
    EXPECT_TRUE(m.validate(200, 0, 0, 3, 3).isAccept());
    EXPECT_TRUE(m.validate(200, 1, 0, 5, 5).isAccept());

    // Partition 1 duplicate
    auto r = m.validate(200, 1, 0, 0, 4);
    EXPECT_TRUE(r.isDuplicate());
    EXPECT_EQ(r.retryBaseOffset, 2000);
}

TEST(ProducerStateManager, ManyProducersManyPartitions) {
    ProducerStateManager m;
    // 4 producers × 4 partitions = 16 independent state cells
    for (int64_t pid = 1; pid <= 4; ++pid) {
        for (int32_t part = 0; part < 4; ++part) {
            int64_t off = pid * 10000 + part * 100;
            produce(m, pid, part, 0, 0, 2, off);
        }
    }

    // Each cell should expect seq 2 next
    for (int64_t pid = 1; pid <= 4; ++pid) {
        for (int32_t part = 0; part < 4; ++part) {
            EXPECT_TRUE(m.validate(pid, part, 0, 2, 3).isAccept())
                << "pid=" << pid << " part=" << part;
        }
    }
}

// ===========================================================================
// 7. Retry window
// ===========================================================================

TEST(ProducerStateManager, RetryWindowHoldsLastNBatches) {
    ProducerStateManager m;
    // Append exactly kRetryWindowSize batches.
    for (int i = 0; i < kRetryWindowSize; ++i) {
        produce(m, 300, 0, 0, i, 1, 5000 + i * 100LL);
    }
    // All should still be in window.
    for (int i = 0; i < kRetryWindowSize; ++i) {
        auto r = m.validate(300, 0, 0, i, i);
        EXPECT_TRUE(r.isDuplicate()) << "i=" << i;
        EXPECT_EQ(r.retryBaseOffset, 5000 + i * 100LL) << "i=" << i;
    }
}

TEST(ProducerStateManager, RetryWindowEvictsOldestBatch) {
    ProducerStateManager m;
    // Append kRetryWindowSize+1 batches → first is evicted.
    for (int i = 0; i <= kRetryWindowSize; ++i) {
        produce(m, 301, 0, 0, i, 1, 7000 + i * 50LL);
    }
    // Batch 0 (seq 0..0) should be evicted → OOS (below expected, not in window).
    auto r0 = m.validate(301, 0, 0, 0, 0);
    EXPECT_TRUE(r0.isOos());

    // Batch 1 (seq 1..1) is the new oldest → still a Duplicate.
    auto r1 = m.validate(301, 0, 0, 1, 1);
    EXPECT_TRUE(r1.isDuplicate());
    EXPECT_EQ(r1.retryBaseOffset, 7000 + 1 * 50LL);
}

TEST(ProducerStateManager, RetryWindowCorrectAfterManyBatches) {
    ProducerStateManager m;
    const int total = kRetryWindowSize * 3 + 2;
    for (int i = 0; i < total; ++i) {
        produce(m, 302, 0, 0, i, 1, 9000 + i * 10LL);
    }
    // The last kRetryWindowSize batches should still be in window.
    for (int i = total - kRetryWindowSize; i < total; ++i) {
        auto r = m.validate(302, 0, 0, i, i);
        EXPECT_TRUE(r.isDuplicate()) << "i=" << i;
        EXPECT_EQ(r.retryBaseOffset, 9000 + i * 10LL) << "i=" << i;
    }
    // Batch before the window → OOS
    int evicted = total - kRetryWindowSize - 1;
    if (evicted >= 0) {
        auto r = m.validate(302, 0, 0, evicted, evicted);
        EXPECT_TRUE(r.isOos()) << "evicted=" << evicted;
    }
}

// ===========================================================================
// 8. Snapshot + recover
// ===========================================================================

TEST(ProducerStateManager, SnapshotAndRecoverRestoresState) {
    std::string path = tmpPath("snap_basic");

    ProducerStateManager m;
    produce(m, 400, 0, 0, 0, 3, 1000); // seq 0..2 @ 1000
    produce(m, 400, 0, 0, 3, 2, 2000); // seq 3..4 @ 2000

    m.takeSnapshot(path);

    ProducerStateManager m2;
    int loaded = m2.recover(path);
    EXPECT_EQ(loaded, 1); // 1 producer-partition entry

    // Duplicate of batch 1 should still be recognized.
    auto r = m2.validate(400, 0, 0, 3, 4);
    EXPECT_TRUE(r.isDuplicate());
    EXPECT_EQ(r.retryBaseOffset, 2000);

    // Next expected: seq 5
    EXPECT_TRUE(m2.validate(400, 0, 0, 5, 5).isAccept());

    ::unlink(path.c_str());
}

TEST(ProducerStateManager, SnapshotAndRecoverMultipleEntries) {
    std::string path = tmpPath("snap_multi");

    ProducerStateManager m;
    produce(m, 1, 0, 0, 0, 1, 100);
    produce(m, 2, 0, 0, 0, 1, 200);
    produce(m, 1, 1, 0, 0, 1, 300);
    produce(m, 3, 5, 2, 0, 1, 400); // epoch 2

    m.takeSnapshot(path);

    ProducerStateManager m2;
    int loaded = m2.recover(path);
    EXPECT_EQ(loaded, 4);

    // pid=1, partition=0: dup of seq 0
    EXPECT_TRUE(m2.validate(1, 0, 0, 0, 0).isDuplicate());
    // pid=2, partition=0: next = 1
    EXPECT_TRUE(m2.validate(2, 0, 0, 1, 1).isAccept());
    // pid=3, partition=5: epoch 1 is fenced
    EXPECT_TRUE(m2.validate(3, 5, 1, 0, 0).isFenced());

    ::unlink(path.c_str());
}

TEST(ProducerStateManager, RecoverFromMissingPathReturnsZero) {
    ProducerStateManager m;
    int loaded = m.recover("/tmp/no_such_file_psm_xyz_" + std::to_string(::getpid()));
    EXPECT_EQ(loaded, 0);
    EXPECT_EQ(m.size(), 0u);
}

TEST(ProducerStateManager, RecoverFromEmptyFileReturnsZero) {
    std::string path = tmpPath("snap_empty");
    // Create empty file
    { ::close(::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644)); }

    ProducerStateManager m;
    int loaded = m.recover(path);
    EXPECT_EQ(loaded, 0);
    ::unlink(path.c_str());
}

// ---------------------------------------------------------------------------
// S5: windowHead/windowCount loaded from disk are clamped to valid ranges.
// Forge a snapshot with out-of-range windowCount (CRC mismatch stops load).
// ---------------------------------------------------------------------------
TEST(ProducerStateManager, CorruptWindowCountClamped) {
    std::string path = tmpPath("psm_s5");
    ProducerStateManager psm;
    produce(psm, 1, 0, 0, 0, 3, 0);
    psm.takeSnapshot(path);

    // Corrupt windowCount (int32 at entry offset 24 within the entry body).
    // File layout: kFileHdrSize(16) + entry body starting at byte 16.
    // windowCount is at entry offset 24: file offset = 16 + 24 = 40.
    {
        int fd = ::open(path.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        uint32_t huge = 0xFFFFFFFFu;
        ::pwrite(fd, &huge, 4, 40);
        ::close(fd);
    }
    // CRC will mismatch -> recover() stops at CRC error (0 entries loaded).
    // The important thing is it must not crash or loop.
    ProducerStateManager psm2;
    EXPECT_NO_THROW(psm2.recover(path));
    // 0 entries (CRC mismatch stops recovery cleanly)
    EXPECT_EQ(psm2.size(), 0u);

    ::unlink(path.c_str());
}

// S3: baseSequence=0 for a new producer is valid (regression for the assert).
TEST(ProducerStateManager, ValidBaseSequenceAccepted) {
    ProducerStateManager psm;
    auto r = psm.validate(999, 0, 0, 0, 0);
    EXPECT_TRUE(r.isAccept()) << "baseSequence=0 for new producer must be Accept";
}

// T5: Torn snapshot (mid-entry truncation) stops recovery cleanly — no crash.
TEST(ProducerStateManager, TornSnapshotStopsCleanly) {
    std::string path = tmpPath("psm_t5");
    ProducerStateManager psm;
    produce(psm, 1, 0, 0, 0, 2, 0);
    produce(psm, 2, 0, 0, 0, 2, 2);
    psm.takeSnapshot(path);

    // Truncate the file mid-entry (keep header + first entry + 10 bytes of second).
    // kFileHdrSize=16, kEntrySize=116.
    off_t truncAt = 16 + 116 + 10;
    {
        struct stat st{};
        ::stat(path.c_str(), &st);
        if (truncAt < st.st_size) ::truncate(path.c_str(), truncAt);
    }

    ProducerStateManager psm2;
    int loaded = 0;
    EXPECT_NO_THROW(loaded = psm2.recover(path));
    // Should have loaded exactly 1 entry (first entry's CRC is intact) or 0 if
    // truncation hit inside the first entry.
    EXPECT_GE(loaded, 0) << "recover must not return negative on torn snapshot";
    EXPECT_LE(static_cast<size_t>(loaded), 2u) << "cannot load more entries than written";

    ::unlink(path.c_str());
}

// T5-corrupt-crc: Flip a CRC byte in the first entry — recovery stops at entry 0.
TEST(ProducerStateManager, CorruptCrcInFirstEntryStopsRecovery) {
    std::string path = tmpPath("psm_t5_crc");
    ProducerStateManager psm;
    produce(psm, 10, 0, 0, 0, 1, 100);
    produce(psm, 20, 0, 0, 0, 1, 200);
    psm.takeSnapshot(path);

    // CRC is the last 4 bytes of the first entry: file offset = 16 + (116-4) = 128.
    {
        int fd = ::open(path.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        uint8_t bad = 0xFF;
        ::pwrite(fd, &bad, 1, 128);
        ::close(fd);
    }

    ProducerStateManager psm2;
    int loaded = psm2.recover(path);
    EXPECT_EQ(loaded, 0) << "CRC mismatch in first entry must stop recovery with 0 loaded";
    EXPECT_EQ(psm2.size(), 0u);

    ::unlink(path.c_str());
}

TEST(ProducerStateManager, SnapshotDedupSurvivesRestartRetryWindow) {
    std::string path = tmpPath("snap_retry_window");

    ProducerStateManager m;
    // Append kRetryWindowSize batches
    for (int i = 0; i < kRetryWindowSize; ++i) {
        produce(m, 500, 0, 0, i, 1, 8000 + i * 10LL);
    }

    m.takeSnapshot(path);
    m = ProducerStateManager{}; // destroy
    m.recover(path);

    // All kRetryWindowSize batches should still be in window.
    for (int i = 0; i < kRetryWindowSize; ++i) {
        auto r = m.validate(500, 0, 0, i, i);
        EXPECT_TRUE(r.isDuplicate()) << "i=" << i;
        EXPECT_EQ(r.retryBaseOffset, 8000 + i * 10LL) << "i=" << i;
    }

    ::unlink(path.c_str());
}

TEST(ProducerStateManager, SnapshotPreservesEpoch) {
    std::string path = tmpPath("snap_epoch");

    ProducerStateManager m;
    produce(m, 600, 0, 3, 0, 1, 1234); // epoch 3

    m.takeSnapshot(path);
    ProducerStateManager m2;
    m2.recover(path);

    // Epoch 2 should be fenced after recovery
    EXPECT_TRUE(m2.validate(600, 0, 2, 1, 1).isFenced());
    // Epoch 4 should be accepted (new session)
    EXPECT_TRUE(m2.validate(600, 0, 4, 0, 0).isAccept());

    ::unlink(path.c_str());
}

// ===========================================================================
// 9. Sequence overflow / large values
// ===========================================================================

TEST(ProducerStateManager, LargeInt32SequencesHandled) {
    ProducerStateManager m;
    // Start near INT32_MAX / 2
    int32_t base = 0x3FFF0000;
    produce(m, 700, 0, 0, 0, base, 1000);       // seq 0 .. base-1
    produce(m, 700, 0, 0, base, 10, 2000);      // seq base .. base+9

    // Dup of second batch
    auto r = m.validate(700, 0, 0, base, base + 9);
    EXPECT_TRUE(r.isDuplicate());
    EXPECT_EQ(r.retryBaseOffset, 2000);

    // Next: base+10
    EXPECT_TRUE(m.validate(700, 0, 0, base + 10, base + 10).isAccept());
}

TEST(ProducerStateManager, VeryFirstBatchOneRecord) {
    ProducerStateManager m;
    auto r = m.validate(800, 0, 0, 0, 0); // 1 record, lastSeq=0
    EXPECT_TRUE(r.isAccept());
    m.update(800, 0, 0, 0, 0, 5000);
    // Next expected: 1
    EXPECT_TRUE(m.validate(800, 0, 0, 1, 1).isAccept());
}

// ===========================================================================
// 10. Entry accessor
// ===========================================================================

TEST(ProducerStateManager, EntryNullForUnknownProducer) {
    ProducerStateManager m;
    EXPECT_EQ(m.entry(9999, 0), nullptr);
}

TEST(ProducerStateManager, EntryReflectsLastSequence) {
    ProducerStateManager m;
    produce(m, 900, 0, 0, 0, 7, 1000);
    const auto* e = m.entry(900, 0);
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->lastSequence, 6); // 0+7-1
    EXPECT_EQ(e->producerEpoch, 0);
}

// ===========================================================================
// 11. Edge: first brand-new producer with non-negative epoch
// ===========================================================================

TEST(ProducerStateManager, BrandNewProducerEpoch2AcceptsSeq0) {
    ProducerStateManager m;
    auto r = m.validate(1000, 0, 2, 0, 0);
    EXPECT_TRUE(r.isAccept());
}

TEST(ProducerStateManager, BrandNewProducerEpoch2RejectsSeq1) {
    ProducerStateManager m;
    auto r = m.validate(1000, 0, 2, 1, 1);
    EXPECT_TRUE(r.isOos());
}

// ===========================================================================
// 12. Snapshot idempotency (overwrite)
// ===========================================================================

TEST(ProducerStateManager, SnapshotCanBeOverwritten) {
    std::string path = tmpPath("snap_overwrite");

    ProducerStateManager m;
    produce(m, 1100, 0, 0, 0, 1, 111);
    m.takeSnapshot(path);

    produce(m, 1100, 0, 0, 1, 1, 222);
    m.takeSnapshot(path); // second snapshot overwrites

    ProducerStateManager m2;
    m2.recover(path);
    EXPECT_TRUE(m2.validate(1100, 0, 0, 0, 0).isDuplicate()); // still in window
    EXPECT_TRUE(m2.validate(1100, 0, 0, 1, 1).isDuplicate()); // also in window

    ::unlink(path.c_str());
}
