// ---------------------------------------------------------------------------
// ProducerIdWatermarkRecoveryTest.cpp — Task 10: ProducerId watermark recovery
//
// Verifies that scanMaxProducerId + recoverProducerIdWatermark correctly
// reseed ProducerIdAllocator so newly-allocated PIDs never collide with or
// regress below PIDs already present in the durable log.
//
// Coverage:
//  1. ReseedsPastHighestScannedPid   — scan finds max PID across batches up to HW;
//                                      allocator is reseeded past it.
//  2. NoObservedPidLeavesAllocatorUnchanged — empty log → allocator untouched.
//  3. BatchAboveHwIsIgnored          — batch whose baseOffset >= HW is NOT counted.
//  4. MonotonicNeverRegresses        — nextId() > maxSeen after recovery.
//  5. TemplatedOverInMemoryLog       — InMemoryLog variant works identically.
//  6. NonIdempotentBatchSkipped      — producerId==-1 batch does not affect scan.
//  7. MultipleProducersBestWins      — multiple PIDs in log → highest wins.
//  8. ReseedIsIdempotent             — calling twice with same log is safe.
// ---------------------------------------------------------------------------

#include <rune/transaction/ProducerIdAllocator.h>
#include <rune/transaction/ProducerIdRecovery.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

using namespace rune::transaction;
using rune::storage::InMemoryLog;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
namespace {

/// Build a valid RecordBatch with a single record, stamping producerId/epoch/seq.
static std::vector<uint8_t> makeBatch(int64_t baseOffset,
                                       int64_t producerId,
                                       int16_t epoch     = 0,
                                       int32_t baseSeq   = 0)
{
    rune::protocol::RecordBatchBuilder b(baseOffset, /*baseTimestamp=*/0);
    b.setProducerId(producerId);
    b.setProducerEpoch(epoch);
    b.setBaseSequence(baseSeq);
    b.append({}, "v", 0);
    return b.build();
}

/// Append one batch to an InMemoryLog; returns the baseOffset used.
static int64_t appendBatch(InMemoryLog& log, int64_t producerId,
                            int16_t epoch = 0, int32_t baseSeq = 0)
{
    const int64_t off = log.logEndOffset();
    auto data = makeBatch(off, producerId, epoch, baseSeq);
    log.append(data.data(), data.size(), off); // single-record → lastOffset == off
    return off;
}

} // anonymous namespace

// ===========================================================================
// 1. Reseed past the highest scanned PID
// ===========================================================================
TEST(ProducerIdWatermarkRecovery, ReseedsPastHighestScannedPid)
{
    InMemoryLog log(1 << 20, 1024);

    // Append batches: PID 1000 at offset 0, PID 5000 at offset 1.
    appendBatch(log, /*pid=*/1000);
    appendBatch(log, /*pid=*/5000);
    // hw=2 covers both batches (baseOffset 0 and 1 are both < 2).
    const int64_t hw = log.logEndOffset(); // == 2

    ProducerIdAllocator alloc(0);
    recoverProducerIdWatermark(alloc, log, hw);

    EXPECT_GT(alloc.nextId(), 5000)
        << "next PID must be strictly greater than recovered max (5000)";
}

// ===========================================================================
// 2. Empty log leaves allocator unchanged
// ===========================================================================
TEST(ProducerIdWatermarkRecovery, NoObservedPidLeavesAllocatorUnchanged)
{
    InMemoryLog emptyLog(1 << 20, 1024);
    ProducerIdAllocator alloc(0);
    recoverProducerIdWatermark(alloc, emptyLog, /*hw=*/0);
    EXPECT_EQ(alloc.nextId(), 0)
        << "empty log: allocator must remain at 0";
}

// ===========================================================================
// 3. Batch above HW is excluded from the scan
// ===========================================================================
TEST(ProducerIdWatermarkRecovery, BatchAboveHwIsIgnored)
{
    InMemoryLog log(1 << 20, 1024);

    appendBatch(log, /*pid=*/100); // offset 0 → inside hw=1
    appendBatch(log, /*pid=*/9999); // offset 1 → outside hw=1

    ProducerIdAllocator alloc(0);
    recoverProducerIdWatermark(alloc, log, /*hw=*/1);

    // PID 9999 must NOT have influenced the allocator.
    EXPECT_GT(alloc.nextId(), 100)
        << "allocator must be seeded past PID 100 (the only committed batch)";
    EXPECT_LE(alloc.nextId(), 9999)
        << "allocator must NOT be seeded past 9999 (that batch is above HW)";
}

// ===========================================================================
// 4. nextId() > maxSeen after recovery (monotonic guarantee, no collision)
// ===========================================================================
TEST(ProducerIdWatermarkRecovery, MonotonicNeverRegresses)
{
    InMemoryLog log(1 << 20, 1024);
    appendBatch(log, /*pid=*/4242);
    const int64_t hw = log.logEndOffset();

    ProducerIdAllocator alloc(0);
    recoverProducerIdWatermark(alloc, log, hw);

    const ProducerId next = alloc.nextId();
    EXPECT_GT(next, 4242)
        << "nextId() must be strictly > highest scanned PID (4242)";

    // Allocate a few IDs and confirm none equal 4242.
    for (int i = 0; i < 5; ++i) {
        ProducerId pid = alloc.allocate();
        EXPECT_NE(pid, 4242) << "allocated PID must not collide with already-used 4242";
        EXPECT_GT(pid, 4242) << "allocated PID must be strictly above scanned max";
    }
}

// ===========================================================================
// 5. Template works identically over InMemoryLog (same as Log for test purposes)
// ===========================================================================
TEST(ProducerIdWatermarkRecovery, TemplatedOverInMemoryLog)
{
    // Explicit: pass an InMemoryLog& to the template — no vtable, no ILog.
    InMemoryLog log(1 << 20, 1024);
    appendBatch(log, /*pid=*/7777);
    const int64_t hw = log.logEndOffset();

    ProducerIdAllocator alloc(0);
    // Explicit template instantiation via the free function template.
    recoverProducerIdWatermark<InMemoryLog>(alloc, log, hw);

    EXPECT_GT(alloc.nextId(), 7777)
        << "InMemoryLog template path must reseed past PID 7777";
}

// ===========================================================================
// 6. Non-idempotent batch (producerId == -1) is skipped
// ===========================================================================
TEST(ProducerIdWatermarkRecovery, NonIdempotentBatchSkipped)
{
    InMemoryLog log(1 << 20, 1024);
    // Append a non-idempotent batch (producerId == -1).
    appendBatch(log, /*pid=*/-1);
    const int64_t hw = log.logEndOffset();

    ProducerIdAllocator alloc(0);
    recoverProducerIdWatermark(alloc, log, hw);

    // No idempotent PID found → allocator should remain at 0.
    EXPECT_EQ(alloc.nextId(), 0)
        << "non-idempotent batch (pid==-1) must not reseed the allocator";
}

// ===========================================================================
// 7. Multiple producers → highest PID wins
// ===========================================================================
TEST(ProducerIdWatermarkRecovery, MultipleProducersBestWins)
{
    InMemoryLog log(1 << 20, 1024);
    appendBatch(log, /*pid=*/300);
    appendBatch(log, /*pid=*/1200);
    appendBatch(log, /*pid=*/800);
    const int64_t hw = log.logEndOffset();

    ProducerIdAllocator alloc(0);
    recoverProducerIdWatermark(alloc, log, hw);

    EXPECT_GT(alloc.nextId(), 1200)
        << "allocator must be seeded past the highest observed PID (1200)";
    EXPECT_LE(alloc.nextId(), 1200 + ProducerIdAllocator::kBlockSize + 1)
        << "allocator should not overshoot by more than one block";
}

// ===========================================================================
// 8. Calling recoverProducerIdWatermark twice is safe (idempotent/monotone)
// ===========================================================================
TEST(ProducerIdWatermarkRecovery, ReseedIsIdempotent)
{
    InMemoryLog log(1 << 20, 1024);
    appendBatch(log, /*pid=*/555);
    const int64_t hw = log.logEndOffset();

    ProducerIdAllocator alloc(0);
    recoverProducerIdWatermark(alloc, log, hw);
    const ProducerId afterFirst = alloc.nextId();

    recoverProducerIdWatermark(alloc, log, hw);
    const ProducerId afterSecond = alloc.nextId();

    EXPECT_EQ(afterFirst, afterSecond)
        << "double reseed with the same log must be idempotent";
}
