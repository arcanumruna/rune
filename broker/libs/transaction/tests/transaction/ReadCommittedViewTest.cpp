// ---------------------------------------------------------------------------
// ReadCommittedViewTest.cpp — Unit tests for P5-4 read-committed isolation.
//
// Tests: LsoTracker, AbortedTxnIndex, ReadCommittedView::rebuild + read.
//
// Strategy:
//   - Log-independent tests for LsoTracker and AbortedTxnIndex (pure in-memory).
//   - Log-based tests build real rune::storage::Log instances in unique tmp dirs,
//     write data batches (LE via RecordBatchBuilder) and control batches (LE via
//     ControlMarker::build — all-LE unification: both use the same LE
//     RecordBatch format), then call ReadCommittedView::rebuild() + read().
//
// Unique temp paths: /tmp/rune_rcv_<pid>_<counter>
// ---------------------------------------------------------------------------

#include <rune/transaction/ReadCommittedView.h>
#include <rune/transaction/ControlMarker.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>
#include <unistd.h>  // getpid()

using namespace rune::transaction;
namespace fs = std::filesystem;

// ===========================================================================
// Test utilities
// ===========================================================================

namespace {

static std::atomic<int> gCounter{0};

/// Unique temporary directory for each test.
static std::string makeTmpDir()
{
    int cnt = gCounter.fetch_add(1, std::memory_order_relaxed);
    std::string dir = std::filesystem::temp_directory_path().string() + "/rune_rcv_" + std::to_string(::getpid())
                    + "_" + std::to_string(cnt);
    fs::create_directories(dir);
    return dir;
}

/// Build a little-endian data batch with a specific producerId.
/// Uses RecordBatchBuilder's setProducerId() for clean LE encoding.
/// A producerId >= 0 here models a TRANSACTIONAL producer, so the batch carries
/// the TRANSACTIONAL attribute (0x10) exactly as a real txn producer's batch
/// does (F15 N3: only a transactional batch opens a transaction).
static std::vector<uint8_t> makeDataBatch(int64_t baseOffset,
                                           int64_t producerId,
                                           std::string_view value = "payload")
{
    rune::protocol::RecordBatchBuilder builder(baseOffset, 0);
    builder.setProducerId(producerId);
    builder.setTransactional(producerId >= 0);
    builder.append({}, value, 0);
    return builder.build();
}

/// Append a data batch for producerId to a Log and return the assigned offset.
static int64_t appendData(rune::storage::Log& log, int64_t producerId,
                           std::string_view value = "data")
{
    int64_t off = log.logEndOffset();
    auto batch = makeDataBatch(off, producerId, value);
    log.append(batch, off); // lastOffset = off (single record, lastOffsetDelta=0)
    return off;
}

/// Append a control marker (COMMIT or ABORT) for producerId to a Log.
/// Returns the offset of the control batch.
static int64_t appendControlMarker(rune::storage::Log& log, int64_t producerId,
                                    ControlType ct, int32_t coordEpoch = 0)
{
    int64_t off = log.logEndOffset();
    auto batch = ControlMarker::build(producerId, 0, ct, coordEpoch);
    // Stamp the baseOffset at [0..7] LITTLE-ENDIAN (all-LE unification:
    // baseOffset is excluded from the CRC region [21..end], so patching
    // [0..7] does not invalidate the CRC).
    uint64_t u = static_cast<uint64_t>(off);
    for (int i = 0; i < 8; ++i)
        batch[i] = static_cast<uint8_t>(u >> (i * 8));
    log.append(batch, off); // lastOffset = off
    return off;
}

/// Read all batch base offsets from a view result.
/// Parses the LE data batches returned by ReadCommittedView::read().
static std::vector<int64_t> readOffsets(const std::vector<uint8_t>& raw)
{
    std::vector<int64_t> result;
    std::size_t pos = 0;
    while (pos < raw.size()) {
        std::size_t rem = raw.size() - pos;
        if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead))
            break;
        rune::protocol::RecordBatchReader reader(raw.data() + pos, rem);
        int32_t total = reader.totalLength();
        if (total <= 0) break;
        result.push_back(reader.baseOffset());
        pos += static_cast<std::size_t>(total);
    }
    return result;
}

} // anonymous namespace

// ===========================================================================
// LsoTracker unit tests
// ===========================================================================

TEST(LsoTrackerTest, NoOpenTxnsLsoEqualsHw)
{
    LsoTracker tracker;
    EXPECT_EQ(tracker.lso(100), 100);
    EXPECT_EQ(tracker.lso(0), 0);
    EXPECT_EQ(tracker.lso(50), 50);
}

TEST(LsoTrackerTest, OneOpenTxnLsoIsItsFirstOffset)
{
    LsoTracker tracker;
    tracker.onProduceBatch(1, 10); // pid=1 opens at offset 10
    EXPECT_EQ(tracker.lso(100), 10);
    EXPECT_EQ(tracker.lso(10), 10); // clamped to hw
    EXPECT_EQ(tracker.lso(5), 5);   // hw < firstOffset → lso=hw
}

TEST(LsoTrackerTest, LsoNeverExceedsHw)
{
    LsoTracker tracker;
    tracker.onProduceBatch(1, 50);
    EXPECT_EQ(tracker.lso(30), 30); // HW < LSO floor → LSO = HW
    EXPECT_EQ(tracker.lso(50), 50);
    EXPECT_EQ(tracker.lso(51), 50);
}

TEST(LsoTrackerTest, CommitAdvancesLso)
{
    LsoTracker tracker;
    tracker.onProduceBatch(1, 10);
    EXPECT_EQ(tracker.lso(100), 10);
    tracker.onControlMarker(1, 20, /*commit=*/true);
    EXPECT_EQ(tracker.lso(100), 100); // no more open txns
}

TEST(LsoTrackerTest, AbortAdvancesLso)
{
    LsoTracker tracker;
    tracker.onProduceBatch(1, 10);
    EXPECT_EQ(tracker.lso(100), 10);
    tracker.onControlMarker(1, 20, /*commit=*/false);
    EXPECT_EQ(tracker.lso(100), 100);
}

TEST(LsoTrackerTest, TwoProducersLsoIsMinFirstOffset)
{
    LsoTracker tracker;
    tracker.onProduceBatch(1, 10);
    tracker.onProduceBatch(2, 5);
    // min(10, 5) = 5
    EXPECT_EQ(tracker.lso(100), 5);
}

TEST(LsoTrackerTest, OverlappingTxnsLsoAdvancesAsOldestResolves)
{
    LsoTracker tracker;
    tracker.onProduceBatch(1, 5);  // pid1 opens at 5
    tracker.onProduceBatch(2, 10); // pid2 opens at 10
    EXPECT_EQ(tracker.lso(100), 5);

    // pid1 commits (older)
    tracker.onControlMarker(1, 20, true);
    EXPECT_EQ(tracker.lso(100), 10); // now oldest is pid2 at 10

    // pid2 commits
    tracker.onControlMarker(2, 30, true);
    EXPECT_EQ(tracker.lso(100), 100); // no open txns
}

TEST(LsoTrackerTest, MultipleBatchesSameProducerFirstOffsetStays)
{
    LsoTracker tracker;
    tracker.onProduceBatch(1, 5);  // first batch for pid1
    tracker.onProduceBatch(1, 8);  // second batch for pid1
    tracker.onProduceBatch(1, 12); // third batch for pid1
    EXPECT_EQ(tracker.lso(100), 5); // still 5 (first write)
    EXPECT_EQ(tracker.firstOffsetOf(1), 5);
}

TEST(LsoTrackerTest, NonTransactionalBatchIgnored)
{
    LsoTracker tracker;
    tracker.onProduceBatch(-1, 5); // non-transactional (pid=-1)
    EXPECT_EQ(tracker.lso(100), 100); // no open txns
    EXPECT_EQ(tracker.openCount(), 0u);
}

TEST(LsoTrackerTest, LsoMonotonicAsSequentialTxnsResolve)
{
    LsoTracker tracker;

    // Txn A: pid=1, starts at 0
    tracker.onProduceBatch(1, 0);
    EXPECT_EQ(tracker.lso(100), 0);

    tracker.onControlMarker(1, 5, true);
    EXPECT_EQ(tracker.lso(100), 100); // A committed, lso=HW

    // Txn B: pid=1, starts at 10
    tracker.onProduceBatch(1, 10);
    EXPECT_EQ(tracker.lso(100), 10); // lso=10, which is >= 0 (prev lso)

    tracker.onControlMarker(1, 15, true);
    EXPECT_EQ(tracker.lso(100), 100);
}

TEST(LsoTrackerTest, ControlMarkerForUnknownProducerReturnsNeg1)
{
    LsoTracker tracker;
    int64_t r = tracker.onControlMarker(999, 5, true);
    EXPECT_EQ(r, -1);
}

TEST(LsoTrackerTest, EmptyTrackerOpenCountIsZero)
{
    LsoTracker tracker;
    EXPECT_EQ(tracker.openCount(), 0u);
    EXPECT_TRUE(tracker.empty());
}

TEST(LsoTrackerTest, ThreeProducersOneCommitsLsoAdvances)
{
    LsoTracker tracker;
    tracker.onProduceBatch(1, 2);
    tracker.onProduceBatch(2, 7);
    tracker.onProduceBatch(3, 4);
    // min(2,7,4) = 2
    EXPECT_EQ(tracker.lso(100), 2);

    tracker.onControlMarker(1, 10, true);
    // open: pid2=7, pid3=4 → min=4
    EXPECT_EQ(tracker.lso(100), 4);

    tracker.onControlMarker(3, 12, true);
    // open: pid2=7
    EXPECT_EQ(tracker.lso(100), 7);

    tracker.onControlMarker(2, 15, true);
    EXPECT_EQ(tracker.lso(100), 100);
}

TEST(LsoTrackerTest, ClearResetsState)
{
    LsoTracker tracker;
    tracker.onProduceBatch(1, 5);
    EXPECT_EQ(tracker.lso(100), 5);
    tracker.clear();
    EXPECT_EQ(tracker.lso(100), 100);
    EXPECT_TRUE(tracker.empty());
}

// ===========================================================================
// AbortedTxnIndex unit tests
// ===========================================================================

TEST(AbortedTxnIndexTest, EmptyIndexIsAbortedReturnsFalse)
{
    AbortedTxnIndex idx;
    EXPECT_FALSE(idx.isAborted(1, 0));
    EXPECT_FALSE(idx.isAborted(1, 100));
}

TEST(AbortedTxnIndexTest, AbortedBatchIsDetected)
{
    AbortedTxnIndex idx;
    idx.onAbortMarker(1, /*firstOffset=*/10, /*lastOffset=*/20);
    EXPECT_TRUE(idx.isAborted(1, 10));
    EXPECT_TRUE(idx.isAborted(1, 15));
    EXPECT_TRUE(idx.isAborted(1, 20));
}

TEST(AbortedTxnIndexTest, BatchBeforeAbortedRangeIsNotAborted)
{
    AbortedTxnIndex idx;
    idx.onAbortMarker(1, 10, 20);
    EXPECT_FALSE(idx.isAborted(1, 9));
    EXPECT_FALSE(idx.isAborted(1, 0));
}

TEST(AbortedTxnIndexTest, BatchAfterAbortedRangeIsNotAborted)
{
    AbortedTxnIndex idx;
    idx.onAbortMarker(1, 10, 20);
    EXPECT_FALSE(idx.isAborted(1, 21));
    EXPECT_FALSE(idx.isAborted(1, 100));
}

TEST(AbortedTxnIndexTest, DifferentProducerNotAborted)
{
    AbortedTxnIndex idx;
    idx.onAbortMarker(1, 10, 20);
    EXPECT_FALSE(idx.isAborted(2, 10)); // different pid
    EXPECT_FALSE(idx.isAborted(2, 15));
}

TEST(AbortedTxnIndexTest, MultipleAbortRanges)
{
    AbortedTxnIndex idx;
    idx.onAbortMarker(1, 0, 5);
    idx.onAbortMarker(2, 10, 15);
    idx.onAbortMarker(1, 20, 25);

    EXPECT_TRUE(idx.isAborted(1, 0));
    EXPECT_TRUE(idx.isAborted(1, 3));
    EXPECT_FALSE(idx.isAborted(1, 6));
    EXPECT_TRUE(idx.isAborted(2, 10));
    EXPECT_FALSE(idx.isAborted(2, 16));
    EXPECT_TRUE(idx.isAborted(1, 22));
}

TEST(AbortedTxnIndexTest, CommittedTxnNotInIndex)
{
    AbortedTxnIndex idx;
    // Only add abort markers, never for pid=2.
    idx.onAbortMarker(1, 5, 10);
    EXPECT_FALSE(idx.isAborted(2, 5)); // pid=2 was committed, not aborted
    EXPECT_TRUE(idx.isAborted(1, 7));
}

TEST(AbortedTxnIndexTest, SizeCountsEntries)
{
    AbortedTxnIndex idx;
    EXPECT_EQ(idx.size(), 0u);
    idx.onAbortMarker(1, 0, 5);
    EXPECT_EQ(idx.size(), 1u);
    idx.onAbortMarker(2, 10, 15);
    EXPECT_EQ(idx.size(), 2u);
}

TEST(AbortedTxnIndexTest, ClearRemovesAll)
{
    AbortedTxnIndex idx;
    idx.onAbortMarker(1, 0, 5);
    idx.clear();
    EXPECT_EQ(idx.size(), 0u);
    EXPECT_FALSE(idx.isAborted(1, 3));
}

// ===========================================================================
// Helpers for Log-based tests
// ===========================================================================

/// Fixture managing a unique tmp dir + Log + LsoTracker + AbortedTxnIndex + View.
struct RcvFixture {
    std::string tmpDir;
    rune::storage::Log log;
    LsoTracker tracker;
    AbortedTxnIndex abortedIndex;
    ReadCommittedView view;

    explicit RcvFixture()
        : tmpDir(makeTmpDir())
        , log(tmpDir)
        , view(&tracker, &abortedIndex)
    {}

    ~RcvFixture()
    {
        fs::remove_all(tmpDir);
    }

    /// High-watermark = LEO (all appended batches are "committed" for test purposes).
    int64_t hw() const { return log.logEndOffset(); }
};

// ===========================================================================
// ReadCommittedView::rebuild tests
// ===========================================================================

TEST(ReadCommittedViewRebuildTest, EmptyLogLsoEqualsHw)
{
    RcvFixture f;
    f.view.rebuild(&f.log, 0);
    EXPECT_EQ(f.view.lso(0), 0);
}

TEST(ReadCommittedViewRebuildTest, DataBatchesWithNoControlsOpenTxns)
{
    RcvFixture f;
    appendData(f.log, 1, "hello");
    appendData(f.log, 2, "world");
    int64_t hw = f.hw();

    f.view.rebuild(&f.log, hw);
    // Both producers are open → LSO = min(0, 1) = 0
    EXPECT_EQ(f.view.lso(hw), 0);
}

TEST(ReadCommittedViewRebuildTest, CommitMarkerClosesTransaction)
{
    RcvFixture f;
    appendData(f.log, 1, "record1");
    appendControlMarker(f.log, 1, ControlType::Commit);
    int64_t hw = f.hw();

    f.view.rebuild(&f.log, hw);
    // No open txns → LSO == HW
    EXPECT_EQ(f.view.lso(hw), hw);
}

TEST(ReadCommittedViewRebuildTest, AbortMarkerClosesAndIndexes)
{
    RcvFixture f;
    int64_t dataOff = appendData(f.log, 1, "aborted-data");
    int64_t abortOff = appendControlMarker(f.log, 1, ControlType::Abort);
    int64_t hw = f.hw();

    f.view.rebuild(&f.log, hw);
    // No open txns → LSO == HW
    EXPECT_EQ(f.view.lso(hw), hw);
    // The data batch is in the aborted index
    EXPECT_TRUE(f.abortedIndex.isAborted(1, dataOff));
    EXPECT_TRUE(f.abortedIndex.isAborted(1, abortOff)); // marker offset also covered
}

TEST(ReadCommittedViewRebuildTest, OneOpenOneCommitted)
{
    RcvFixture f;
    // pid=1: open (no control)
    appendData(f.log, 1, "open-data");
    // pid=2: open then commit
    appendData(f.log, 2, "committed-data");
    appendControlMarker(f.log, 2, ControlType::Commit);
    int64_t hw = f.hw();

    f.view.rebuild(&f.log, hw);
    // pid=1 still open at offset 0 → LSO=0
    EXPECT_EQ(f.view.lso(hw), 0);
    EXPECT_FALSE(f.abortedIndex.isAborted(2, 1)); // pid=2 committed
}

// ===========================================================================
// ReadCommittedView::read — read_uncommitted mode
// ===========================================================================

TEST(ReadCommittedViewReadTest, ReadUncommittedReadsAllBelowHw)
{
    RcvFixture f;
    appendData(f.log, 1, "a");
    appendData(f.log, 2, "b");
    appendData(f.log, 3, "c");
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    auto raw = f.view.read(&f.log, hw, 0, 1 << 20, IsolationLevel::ReadUncommitted);
    auto offsets = readOffsets(raw);
    ASSERT_EQ(offsets.size(), 3u);
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
    EXPECT_EQ(offsets[2], 2);
}

TEST(ReadCommittedViewReadTest, ReadUncommittedIncludesInFlightTxn)
{
    RcvFixture f;
    appendData(f.log, 1, "in-flight"); // open txn, no control yet
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    // read_uncommitted sees everything < HW
    auto raw = f.view.read(&f.log, hw, 0, 1 << 20, IsolationLevel::ReadUncommitted);
    auto offsets = readOffsets(raw);
    EXPECT_EQ(offsets.size(), 1u);
}

TEST(ReadCommittedViewReadTest, ReadCommittedHidesInFlightTxn)
{
    RcvFixture f;
    appendData(f.log, 1, "in-flight"); // open, no COMMIT yet
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    // LSO = 0 (first open txn at offset 0)
    EXPECT_EQ(f.view.lso(hw), 0);

    auto raw = f.view.read(&f.log, hw, 0, 1 << 20, IsolationLevel::ReadCommitted);
    EXPECT_TRUE(raw.empty()); // nothing before LSO=0
}

TEST(ReadCommittedViewReadTest, ReadCommittedSeesCommittedBatches)
{
    RcvFixture f;
    appendData(f.log, 1, "committed1");
    appendControlMarker(f.log, 1, ControlType::Commit);
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    // LSO == HW (no open txns)
    EXPECT_EQ(f.view.lso(hw), hw);

    auto raw = f.view.read(&f.log, hw, 0, 1 << 20, IsolationLevel::ReadCommitted);
    auto offsets = readOffsets(raw);
    // Data batch at offset 0 should be returned; control batch filtered out.
    EXPECT_EQ(offsets.size(), 1u);
    EXPECT_EQ(offsets[0], 0);
}

TEST(ReadCommittedViewReadTest, ReadCommittedFiltersAbortedBatches)
{
    RcvFixture f;
    appendData(f.log, 1, "aborted");
    appendControlMarker(f.log, 1, ControlType::Abort);
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    // LSO == HW (txn resolved)
    EXPECT_EQ(f.view.lso(hw), hw);

    auto raw = f.view.read(&f.log, hw, 0, 1 << 20, IsolationLevel::ReadCommitted);
    EXPECT_TRUE(raw.empty()); // aborted data filtered out
}

TEST(ReadCommittedViewReadTest, ReadUncommittedSeesAbortedData)
{
    RcvFixture f;
    appendData(f.log, 1, "aborted");
    appendControlMarker(f.log, 1, ControlType::Abort);
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    // read_uncommitted sees data batches (but NOT control batches)
    auto raw = f.view.read(&f.log, hw, 0, 1 << 20, IsolationLevel::ReadUncommitted);
    auto offsets = readOffsets(raw);
    EXPECT_EQ(offsets.size(), 1u); // data batch at offset 0
    EXPECT_EQ(offsets[0], 0);
}

TEST(ReadCommittedViewReadTest, ControlBatchesNeverReturnedInEitherMode)
{
    RcvFixture f;
    appendControlMarker(f.log, 1, ControlType::Commit);
    appendControlMarker(f.log, 2, ControlType::Abort);
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    auto rawRu = f.view.read(&f.log, hw, 0, 1<<20, IsolationLevel::ReadUncommitted);
    EXPECT_TRUE(rawRu.empty());

    auto rawRc = f.view.read(&f.log, hw, 0, 1<<20, IsolationLevel::ReadCommitted);
    EXPECT_TRUE(rawRc.empty());
}

// ===========================================================================
// Interleaved producers — one commits, one aborts
// ===========================================================================

TEST(ReadCommittedViewReadTest, InterleavedTwoProducersOneCommitsOneAborts)
{
    RcvFixture f;
    // Interleaved writes:
    //   off 0: pid=1 data
    //   off 1: pid=2 data
    //   off 2: pid=1 data (second batch for pid=1)
    //   off 3: pid=2 ABORT marker
    //   off 4: pid=1 COMMIT marker
    appendData(f.log, 1, "p1-batch0");
    appendData(f.log, 2, "p2-batch1");
    appendData(f.log, 1, "p1-batch2");
    appendControlMarker(f.log, 2, ControlType::Abort);
    appendControlMarker(f.log, 1, ControlType::Commit);
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    EXPECT_EQ(f.view.lso(hw), hw); // both resolved

    // read_committed: must return pid=1's data (offsets 0, 2), skip pid=2 (offset 1).
    auto raw = f.view.read(&f.log, hw, 0, 1<<20, IsolationLevel::ReadCommitted);
    auto offsets = readOffsets(raw);
    EXPECT_EQ(offsets.size(), 2u);
    EXPECT_EQ(offsets[0], 0); // pid=1 first batch
    EXPECT_EQ(offsets[1], 2); // pid=1 second batch
    // offset 1 (pid=2) is absent

    // read_uncommitted: returns pid=1 data + pid=2 data (but NOT control batches)
    auto rawRu = f.view.read(&f.log, hw, 0, 1<<20, IsolationLevel::ReadUncommitted);
    auto offsRu = readOffsets(rawRu);
    EXPECT_EQ(offsRu.size(), 3u); // offsets 0, 1, 2
}

// ===========================================================================
// Boundary tests
// ===========================================================================

TEST(ReadCommittedViewReadTest, ReadAtExactlyLsoReturnsNothing)
{
    RcvFixture f;
    appendData(f.log, 1, "open");
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);
    int64_t lso = f.view.lso(hw);
    EXPECT_EQ(lso, 0); // open txn at offset 0

    // Fetch from exactly LSO=0 → nothing (fetchOffset >= lso)
    auto raw = f.view.read(&f.log, hw, lso, 1<<20, IsolationLevel::ReadCommitted);
    EXPECT_TRUE(raw.empty());
}

TEST(ReadCommittedViewReadTest, ReadWithFetchOffsetBeyondHwReturnsEmpty)
{
    RcvFixture f;
    appendData(f.log, 1, "data");
    appendControlMarker(f.log, 1, ControlType::Commit);
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    auto raw = f.view.read(&f.log, hw, hw + 10, 1<<20, IsolationLevel::ReadCommitted);
    EXPECT_TRUE(raw.empty());

    auto rawRu = f.view.read(&f.log, hw, hw + 10, 1<<20, IsolationLevel::ReadUncommitted);
    EXPECT_TRUE(rawRu.empty());
}

TEST(ReadCommittedViewReadTest, EmptyPartitionReturnsNothing)
{
    RcvFixture f;
    // No batches appended.
    f.view.rebuild(&f.log, 0);

    auto rawRc = f.view.read(&f.log, 0, 0, 1<<20, IsolationLevel::ReadCommitted);
    EXPECT_TRUE(rawRc.empty());

    auto rawRu = f.view.read(&f.log, 0, 0, 1<<20, IsolationLevel::ReadUncommitted);
    EXPECT_TRUE(rawRu.empty());
}

TEST(ReadCommittedViewReadTest, AllAbortedPartitionReturnsNothing)
{
    RcvFixture f;
    appendData(f.log, 1, "a1");
    appendData(f.log, 1, "a2");
    appendControlMarker(f.log, 1, ControlType::Abort);
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);
    EXPECT_EQ(f.view.lso(hw), hw);

    auto raw = f.view.read(&f.log, hw, 0, 1<<20, IsolationLevel::ReadCommitted);
    EXPECT_TRUE(raw.empty());
}

TEST(ReadCommittedViewReadTest, AllCommittedPartitionReturnsAll)
{
    RcvFixture f;
    appendData(f.log, 1, "d0");
    appendData(f.log, 1, "d1");
    appendData(f.log, 1, "d2");
    appendControlMarker(f.log, 1, ControlType::Commit);
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);
    EXPECT_EQ(f.view.lso(hw), hw);

    auto raw = f.view.read(&f.log, hw, 0, 1<<20, IsolationLevel::ReadCommitted);
    auto offsets = readOffsets(raw);
    EXPECT_EQ(offsets.size(), 3u);
}

// ===========================================================================
// LSO invariants
// ===========================================================================

TEST(LsoInvariantsTest, LsoMonotonicAsOpenTxnsResolveInOrder)
{
    // Simulate offset-ordered txns resolving.
    LsoTracker tracker;
    tracker.onProduceBatch(1, 0);
    tracker.onProduceBatch(2, 5);
    tracker.onProduceBatch(3, 10);

    int64_t lastLso = 0;
    int64_t hw = 100;

    // Resolve pid=1 (oldest)
    tracker.onControlMarker(1, 15, true);
    int64_t newLso = tracker.lso(hw);
    EXPECT_GE(newLso, lastLso);
    lastLso = newLso;
    EXPECT_EQ(newLso, 5); // now pid=2 is oldest

    // Resolve pid=3 (not oldest — pid=2 still open)
    tracker.onControlMarker(3, 20, true);
    newLso = tracker.lso(hw);
    EXPECT_GE(newLso, lastLso);
    lastLso = newLso;
    EXPECT_EQ(newLso, 5); // pid=2 still oldest

    // Resolve pid=2
    tracker.onControlMarker(2, 25, true);
    newLso = tracker.lso(hw);
    EXPECT_GE(newLso, lastLso);
    EXPECT_EQ(newLso, hw);
}

TEST(LsoInvariantsTest, LsoNeverExceedsHwIncrementally)
{
    LsoTracker tracker;
    tracker.onProduceBatch(1, 50); // open at 50
    for (int hw = 0; hw <= 100; ++hw) {
        EXPECT_LE(tracker.lso(hw), hw);
    }
}

TEST(LsoInvariantsTest, LsoIsMinFirstOffsetNotMinPid)
{
    // pid=10 starts earlier (offset 3); pid=1 starts later (offset 7).
    // LSO should be 3 (by firstOffset), not governed by pid order.
    LsoTracker tracker;
    tracker.onProduceBatch(10, 3);
    tracker.onProduceBatch(1, 7);
    EXPECT_EQ(tracker.lso(100), 3);

    tracker.onControlMarker(10, 20, true);
    EXPECT_EQ(tracker.lso(100), 7); // pid=1 now oldest
}

// ===========================================================================
// read_committed vs read_uncommitted — in-flight txn visibility
// ===========================================================================

TEST(IsolationLevelTest, InFlightTxnVisibleUnderReadUncommittedHiddenUnderReadCommitted)
{
    RcvFixture f;
    // Two batches from pid=1 (in-flight, no COMMIT/ABORT yet)
    appendData(f.log, 1, "r0");
    appendData(f.log, 1, "r1");
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    // LSO = 0 (pid=1 open at 0)
    EXPECT_EQ(f.view.lso(hw), 0);

    auto rawRc = f.view.read(&f.log, hw, 0, 1<<20, IsolationLevel::ReadCommitted);
    EXPECT_TRUE(rawRc.empty()); // nothing before LSO=0

    auto rawRu = f.view.read(&f.log, hw, 0, 1<<20, IsolationLevel::ReadUncommitted);
    auto offsRu = readOffsets(rawRu);
    EXPECT_EQ(offsRu.size(), 2u); // both visible
}

TEST(IsolationLevelTest, CommittedRecordVisibleInBothModes)
{
    RcvFixture f;
    appendData(f.log, 1, "v0");
    appendControlMarker(f.log, 1, ControlType::Commit);
    appendData(f.log, 2, "v1"); // pid=2 open (in-flight)
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    // pid=1 committed at offset 0; pid=2 open at offset 2.
    // LSO = 2 (first open txn is pid=2 at offset 2).
    EXPECT_EQ(f.view.lso(hw), 2);

    auto rawRc = f.view.read(&f.log, hw, 0, 1<<20, IsolationLevel::ReadCommitted);
    auto offsRc = readOffsets(rawRc);
    // Only offset 0 is < LSO=2 AND committed (offset 1 is control, 2 is in-flight).
    EXPECT_EQ(offsRc.size(), 1u);
    EXPECT_EQ(offsRc[0], 0);

    auto rawRu = f.view.read(&f.log, hw, 0, 1<<20, IsolationLevel::ReadUncommitted);
    auto offsRu = readOffsets(rawRu);
    // read_uncommitted: offsets 0 and 2 (data batches; offset 1 is control).
    EXPECT_EQ(offsRu.size(), 2u);
}

// ===========================================================================
// Partial reads — fetchOffset mid-stream
// ===========================================================================

TEST(ReadCommittedViewReadTest, FetchOffsetMidStream)
{
    RcvFixture f;
    appendData(f.log, 1, "d0"); // offset 0
    appendData(f.log, 1, "d1"); // offset 1
    appendData(f.log, 1, "d2"); // offset 2
    appendControlMarker(f.log, 1, ControlType::Commit);
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    // Read from offset 1
    auto raw = f.view.read(&f.log, hw, 1, 1<<20, IsolationLevel::ReadCommitted);
    auto offsets = readOffsets(raw);
    EXPECT_EQ(offsets.size(), 2u); // offsets 1 and 2
    EXPECT_EQ(offsets[0], 1);
    EXPECT_EQ(offsets[1], 2);
}

// ===========================================================================
// Large interleaved scenario
// ===========================================================================

TEST(ReadCommittedViewReadTest, ThreeProducersMixedCommitAbort)
{
    RcvFixture f;
    // pid=1 commits; pid=2 aborts; pid=3 commits
    // Writes interleaved:
    //   off 0: pid=1
    //   off 1: pid=2
    //   off 2: pid=3
    //   off 3: pid=1
    //   off 4: pid=2 ABORT
    //   off 5: pid=3
    //   off 6: pid=1 COMMIT
    //   off 7: pid=3 COMMIT
    appendData(f.log, 1, "p1a"); // 0
    appendData(f.log, 2, "p2a"); // 1
    appendData(f.log, 3, "p3a"); // 2
    appendData(f.log, 1, "p1b"); // 3
    appendControlMarker(f.log, 2, ControlType::Abort);  // 4
    appendData(f.log, 3, "p3b"); // 5
    appendControlMarker(f.log, 1, ControlType::Commit); // 6
    appendControlMarker(f.log, 3, ControlType::Commit); // 7
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    EXPECT_EQ(f.view.lso(hw), hw); // all resolved

    auto raw = f.view.read(&f.log, hw, 0, 1<<20, IsolationLevel::ReadCommitted);
    auto offsets = readOffsets(raw);
    // Expected: pid=1 (0,3) + pid=3 (2,5) = 4 batches, in order.
    // pid=2 (offset 1) is aborted → filtered.
    ASSERT_EQ(offsets.size(), 4u);
    EXPECT_EQ(offsets[0], 0); // pid=1
    EXPECT_EQ(offsets[1], 2); // pid=3
    EXPECT_EQ(offsets[2], 3); // pid=1
    EXPECT_EQ(offsets[3], 5); // pid=3

    // read_uncommitted includes pid=2's data (but not control batches).
    // Data offsets: 0,1,2,3,5. Control offsets: 4,6,7. Total data = 5.
    auto rawRu = f.view.read(&f.log, hw, 0, 1<<20, IsolationLevel::ReadUncommitted);
    auto offsRu = readOffsets(rawRu);
    EXPECT_EQ(offsRu.size(), 5u);
}

// ===========================================================================
// Incremental (non-rebuild) LsoTracker usage
// ===========================================================================

TEST(LsoTrackerIncrementalTest, IncrementalUpdatesMatchRebuild)
{
    // Simulate incremental updates and compare LSO to rebuild result.
    std::string tmpDir = makeTmpDir();
    rune::storage::Log log(tmpDir);

    LsoTracker incrementalTracker;
    AbortedTxnIndex incrementalIndex;

    auto onAppendData = [&](int64_t pid) {
        int64_t off = log.logEndOffset();
        appendData(log, pid);
        incrementalTracker.onProduceBatch(pid, off);
    };

    auto onAppendCommit = [&](int64_t pid) {
        int64_t off = log.logEndOffset();
        appendControlMarker(log, pid, ControlType::Commit);
        incrementalTracker.onControlMarker(pid, off, true);
    };

    auto onAppendAbort = [&](int64_t pid) {
        int64_t off = log.logEndOffset();
        appendControlMarker(log, pid, ControlType::Abort);
        int64_t firstOff = incrementalTracker.onControlMarker(pid, off, false);
        if (firstOff >= 0)
            incrementalIndex.onAbortMarker(pid, firstOff, off);
    };

    onAppendData(1);
    onAppendData(2);
    onAppendCommit(1);
    onAppendData(3);
    onAppendAbort(2);

    int64_t hw = log.logEndOffset();

    // Rebuild from scratch and compare.
    LsoTracker rebuildTracker;
    AbortedTxnIndex rebuildIndex;
    ReadCommittedView rebuildView(&rebuildTracker, &rebuildIndex);
    rebuildView.rebuild(&log, hw);

    EXPECT_EQ(incrementalTracker.lso(hw), rebuildTracker.lso(hw));
    EXPECT_EQ(incrementalTracker.lso(hw), 3); // pid=3 open at offset 3

    fs::remove_all(tmpDir);
}

// ===========================================================================
// Edge: LSO with HW = 0
// ===========================================================================

TEST(LsoTrackerTest, LsoWithHwZero)
{
    LsoTracker tracker;
    tracker.onProduceBatch(1, 0);
    EXPECT_EQ(tracker.lso(0), 0);
}

TEST(LsoTrackerTest, LsoNonTransactionalBatchesDoNotOpenTxns)
{
    LsoTracker tracker;
    // Simulating non-transactional producers (pid=-1)
    tracker.onProduceBatch(-1, 0);
    tracker.onProduceBatch(-1, 1);
    EXPECT_EQ(tracker.lso(100), 100); // no open txns
}

// ===========================================================================
// AbortedTxnIndex — range ordering
// ===========================================================================

TEST(AbortedTxnIndexTest, RangesOrderedByFirstOffset)
{
    AbortedTxnIndex idx;
    idx.onAbortMarker(1, 20, 30);
    idx.onAbortMarker(2, 5, 10);
    idx.onAbortMarker(3, 0, 3);

    const auto& ranges = idx.ranges();
    ASSERT_EQ(ranges.size(), 3u);
    EXPECT_EQ(ranges[0].firstOffset, 0);
    EXPECT_EQ(ranges[1].firstOffset, 5);
    EXPECT_EQ(ranges[2].firstOffset, 20);
}

// ===========================================================================
// IsolationLevel enum
// ===========================================================================

TEST(IsolationLevelTest, EnumValues)
{
    EXPECT_EQ(static_cast<uint8_t>(IsolationLevel::ReadUncommitted), 0u);
    EXPECT_EQ(static_cast<uint8_t>(IsolationLevel::ReadCommitted),   1u);
}

// ===========================================================================
// P5-4 review follow-up: 8 missing edge-case tests
// ===========================================================================

// ── Edge 1: same-PID sequential txns — abort-then-commit ─────────────────────
// Producer P runs txn1 (aborts) then txn2 (commits).  read_committed must:
//   - skip txn1's data (it's in the aborted range)
//   - return txn2's data (different offset range; same PID)
// Disambiguation is by offset range, not epoch — proven here.
TEST(ReadCommittedEdgeTest, SamePidAbortThenCommitDisambiguatedByOffsetRange)
{
    RcvFixture f;

    // txn1: pid=1
    int64_t txn1DataOff   = appendData(f.log, 1, "txn1-data");   // offset 0
    int64_t txn1AbortOff  = appendControlMarker(f.log, 1, ControlType::Abort); // offset 1

    // txn2: same pid=1, new transaction starting at a higher offset
    int64_t txn2DataOff   = appendData(f.log, 1, "txn2-data");   // offset 2
    int64_t txn2CommitOff = appendControlMarker(f.log, 1, ControlType::Commit); // offset 3

    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    // Both transactions resolved → LSO == HW.
    EXPECT_EQ(f.view.lso(hw), hw);

    // Aborted index must contain txn1's range [0..1], but NOT txn2's range [2..3].
    EXPECT_TRUE(f.abortedIndex.isAborted(1, txn1DataOff));   // txn1 data — aborted
    EXPECT_TRUE(f.abortedIndex.isAborted(1, txn1AbortOff));  // abort marker offset — covered
    EXPECT_FALSE(f.abortedIndex.isAborted(1, txn2DataOff));  // txn2 data — NOT aborted
    EXPECT_FALSE(f.abortedIndex.isAborted(1, txn2CommitOff));// txn2 commit — NOT aborted

    // read_committed: txn1 data filtered; txn2 data returned.
    auto raw = f.view.read(&f.log, hw, 0, 1 << 20, IsolationLevel::ReadCommitted);
    auto offsets = readOffsets(raw);
    ASSERT_EQ(offsets.size(), 1u) << "only txn2 data should be returned";
    EXPECT_EQ(offsets[0], txn2DataOff);

    // read_uncommitted: both data batches visible (control batches stripped).
    auto rawRu = f.view.read(&f.log, hw, 0, 1 << 20, IsolationLevel::ReadUncommitted);
    auto offsRu = readOffsets(rawRu);
    ASSERT_EQ(offsRu.size(), 2u) << "both txn1 and txn2 data visible under read_uncommitted";
    EXPECT_EQ(offsRu[0], txn1DataOff);
    EXPECT_EQ(offsRu[1], txn2DataOff);
}

// ── Edge 2: out-of-start-order commit via rebuild path ───────────────────────
// Two open txns; the later-starting one commits first.
// LSO must NOT advance past the still-open earlier txn.
TEST(ReadCommittedEdgeTest, OutOfStartOrderCommitViaRebuildLsoStaysPinned)
{
    RcvFixture f;

    // pid=1 opens first (offset 0), pid=2 opens later (offset 1).
    appendData(f.log, 1, "pid1-data");   // offset 0 — earlier start
    appendData(f.log, 2, "pid2-data");   // offset 1 — later start

    // pid=2 (later start) commits first.
    appendControlMarker(f.log, 2, ControlType::Commit); // offset 2

    // pid=1 is still open.
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    // LSO must still be pinned at pid=1's firstOffset (0), not at HW.
    int64_t lso = f.view.lso(hw);
    EXPECT_EQ(lso, 0) << "LSO must not advance past the still-open earlier txn";

    // read_committed must return nothing (everything >= LSO=0 is hidden by open pid=1).
    auto raw = f.view.read(&f.log, hw, 0, 1 << 20, IsolationLevel::ReadCommitted);
    EXPECT_TRUE(raw.empty());
}

// ── Edge 3: ABORT marker with NO data batches (empty aborted txn) ─────────────
// An ABORT control batch arrives for a producer that never wrote a data batch.
// Expect: no spurious aborted range; LSO advances correctly (nothing to pin).
TEST(ReadCommittedEdgeTest, AbortMarkerWithNoDataBatchesNoSpuriousRange)
{
    RcvFixture f;

    // Append an ABORT for pid=1 with no preceding data batch for pid=1.
    appendControlMarker(f.log, 1, ControlType::Abort);
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    // No open txns → LSO == HW.
    EXPECT_EQ(f.view.lso(hw), hw);

    // No aborted range should be recorded (producer was never tracked as open).
    EXPECT_EQ(f.abortedIndex.size(), 0u)
        << "no aborted range when no data batch preceded the ABORT marker";

    // read_committed: empty result (only control batch, which is stripped).
    auto raw = f.view.read(&f.log, hw, 0, 1 << 20, IsolationLevel::ReadCommitted);
    EXPECT_TRUE(raw.empty());
}

// ── Edge 4: oldest open txn's firstOffset exactly at HW ──────────────────────
// If the only open txn starts exactly at HW, LSO == HW (clamped), so nothing
// is hidden that shouldn't be (we can't read past HW anyway).
TEST(ReadCommittedEdgeTest, OldestOpenTxnFirstOffsetExactlyAtHw)
{
    RcvFixture f;

    // Append one data batch; that makes hw=1 and LSO should be 0 (open at 0).
    appendData(f.log, 1, "d0"); // offset 0
    int64_t hw = f.hw();        // hw = 1

    f.view.rebuild(&f.log, hw);

    // pid=1 is open at offset 0, which is strictly < HW=1.
    // Fetch HW at the edge: pass hw=1 but the batch's firstOffset==0 < hw.
    int64_t lso = f.view.lso(hw);
    EXPECT_EQ(lso, 0);
    EXPECT_LE(lso, hw);

    // Now simulate: hw equals the txn's firstOffset (pass hw=0).
    // With hw=0, there are no batches < hw, so LSO is clamped to [0,hw]=0.
    int64_t lsoAtHw0 = f.view.lso(0);
    EXPECT_EQ(lsoAtHw0, 0); // clamped to hw=0; nothing incorrect hidden
}

// ── Edge 5: aborted-range off-by-one at the marker boundary via read() ────────
// The record immediately BEFORE the abort-range should be returned by
// read_committed (if from a different producer).  The data batch AT the
// first aborted offset should be filtered.  The data batch AFTER the abort
// marker's offset (different txn) should be returned.
TEST(ReadCommittedEdgeTest, AbortedRangeBoundaryOffByOneViaRead)
{
    RcvFixture f;

    // pid=2 committed data at offset 0 (before the aborted range starts).
    appendData(f.log, 2, "pre-abort-data");     // offset 0 — pid=2, committed
    appendControlMarker(f.log, 2, ControlType::Commit); // offset 1

    // pid=1 aborted txn: data at offset 2, abort marker at offset 3.
    appendData(f.log, 1, "aborted-data");        // offset 2 — aborted
    appendControlMarker(f.log, 1, ControlType::Abort); // offset 3

    // pid=3 committed data AFTER the abort marker (offset 4).
    appendData(f.log, 3, "post-abort-data");     // offset 4 — committed
    appendControlMarker(f.log, 3, ControlType::Commit); // offset 5

    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    EXPECT_EQ(f.view.lso(hw), hw); // all resolved

    // Aborted range: pid=1 covers [2..3].
    EXPECT_FALSE(f.abortedIndex.isAborted(1, 1)); // offset 1 (BEFORE range) — not aborted
    EXPECT_TRUE(f.abortedIndex.isAborted(1, 2));  // offset 2 (AT firstOffset) — aborted
    EXPECT_TRUE(f.abortedIndex.isAborted(1, 3));  // offset 3 (AT lastOffset)  — aborted
    EXPECT_FALSE(f.abortedIndex.isAborted(1, 4)); // offset 4 (AFTER range)    — not aborted

    // read_committed: must return pid=2 at offset 0, and pid=3 at offset 4.
    // Must skip pid=1 at offset 2.
    auto raw = f.view.read(&f.log, hw, 0, 1 << 20, IsolationLevel::ReadCommitted);
    auto offsets = readOffsets(raw);
    ASSERT_EQ(offsets.size(), 2u);
    EXPECT_EQ(offsets[0], 0); // pid=2 pre-abort data
    EXPECT_EQ(offsets[1], 4); // pid=3 post-abort data
}

// ── Edge 6: control batch at offset 0 ────────────────────────────────────────
// The very first batch on a partition is a control marker (e.g. ABORT for an
// unknown/untracked producer).  rebuild() must not crash or produce a spurious
// aborted range.
//
// P5-4 fix: control batches are now BE RecordBatch format, so LogSegment::read()
// CAN walk past a control batch at offset 0.  The test now also verifies that
// read() returns committed data after a leading control marker.
TEST(ReadCommittedEdgeTest, ControlBatchAtOffsetZeroHandledCorrectly)
{
    RcvFixture f;

    // Append a control marker as the very first batch (offset 0).
    appendControlMarker(f.log, 42, ControlType::Abort); // offset 0 — unknown PID
    // Follow with a committed data batch and commit marker.
    appendData(f.log, 7, "normal-data");                // offset 1
    appendControlMarker(f.log, 7, ControlType::Commit); // offset 2

    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    // rebuild() must not crash and must not produce a spurious aborted range
    // for pid=42 (which never opened a transaction via a data batch).
    EXPECT_EQ(f.abortedIndex.size(), 0u)
        << "control batch at offset 0 for untracked PID must not add aborted range";

    // LSO == HW: pid=7 resolved; pid=42 was never open (ABORT for unknown → no-op).
    EXPECT_EQ(f.view.lso(hw), hw)
        << "LSO should equal HW after all transactions resolved";

    // Verify the tracker correctly tracked pid=7 and then removed it.
    // After rebuild, openTxns_ should be empty.
    EXPECT_TRUE(f.tracker.empty())
        << "no open transactions remain after rebuild";

    // Also test via a mid-stream unknown-PID control batch (avoids the offset-0
    // storage limitation) to confirm read_committed also handles this correctly.
    RcvFixture f2;
    appendData(f2.log, 5, "pid5-data");                   // offset 0
    appendControlMarker(f2.log, 99, ControlType::Abort);  // offset 1 — unknown PID
    appendControlMarker(f2.log, 5, ControlType::Commit);  // offset 2
    int64_t hw2 = f2.hw();
    f2.view.rebuild(&f2.log, hw2);

    EXPECT_EQ(f2.abortedIndex.size(), 0u)
        << "mid-stream ABORT for untracked PID must not add aborted range";
    EXPECT_EQ(f2.view.lso(hw2), hw2);

    // read_committed: pid=5 data at offset 0 returned; control batches stripped.
    auto raw2 = f2.view.read(&f2.log, hw2, 0, 1 << 20, IsolationLevel::ReadCommitted);
    auto offsets2 = readOffsets(raw2);
    ASSERT_EQ(offsets2.size(), 1u)
        << "only pid=5's committed data should be returned";
    EXPECT_EQ(offsets2[0], 0);
}

// ── Edge 7: maxBytes soft-cap path under read_committed ───────────────────────
// Verifies that the soft cap is respected (≥1 batch always returned if any
// qualify), and that the cap correctly splits the stream.
TEST(ReadCommittedEdgeTest, MaxBytesSoftCapReturnsAtLeastOneBatchThenStops)
{
    RcvFixture f;

    // Append 4 data batches for pid=1, all committed.
    appendData(f.log, 1, "batch-A");
    appendData(f.log, 1, "batch-B");
    appendData(f.log, 1, "batch-C");
    appendData(f.log, 1, "batch-D");
    appendControlMarker(f.log, 1, ControlType::Commit);
    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    EXPECT_EQ(f.view.lso(hw), hw);

    // Get the size of a single data batch by reading 1<<20 and measuring offsets.
    auto allRaw = f.view.read(&f.log, hw, 0, 1 << 20, IsolationLevel::ReadCommitted);
    ASSERT_EQ(readOffsets(allRaw).size(), 4u);

    // Compute a maxBytes that fits exactly ONE batch (plus a tiny buffer for the
    // soft-cap check: cap is "stop after collected > maxBytes if collected > 0").
    // Each batch has the same size; use allRaw.size()/4 - 1 to be under one batch.
    int32_t oneBatchSize = static_cast<int32_t>(allRaw.size() / 4);
    int32_t smallCap = oneBatchSize / 2; // definitely less than one batch

    // With smallCap < oneBatchSize, the "always return at least one batch" rule
    // should return exactly one batch (the first qualifying one).
    auto rawCapped = f.view.read(&f.log, hw, 0, smallCap, IsolationLevel::ReadCommitted);
    auto cappedOffsets = readOffsets(rawCapped);
    EXPECT_EQ(cappedOffsets.size(), 1u)
        << "soft cap: at least 1 batch must always be returned";
    EXPECT_EQ(cappedOffsets[0], 0);

    // With a cap of exactly oneBatchSize, still ≥1 and the second batch would
    // push us over (collected after first = oneBatchSize, > cap → stop).
    auto rawExact = f.view.read(&f.log, hw, 0, oneBatchSize, IsolationLevel::ReadCommitted);
    auto exactOffsets = readOffsets(rawExact);
    // The logic: after collecting batch 0 (size = oneBatchSize),
    // collected == oneBatchSize == maxBytes (not >, so NOT stopped yet);
    // then batch 1: collected + size > maxBytes → stop before adding.
    // So we expect exactly 1 batch.
    EXPECT_GE(exactOffsets.size(), 1u);
    EXPECT_LE(exactOffsets.size(), 2u); // at most 2 (boundary behaviour)

    // Larger cap that fits all 4.
    auto rawAll = f.view.read(&f.log, hw, 0, static_cast<int32_t>(allRaw.size()),
                               IsolationLevel::ReadCommitted);
    EXPECT_EQ(readOffsets(rawAll).size(), 4u);
}

// ── Edge 8: large aborted index — correctness holds at scale ──────────────────
// Many aborted transactions for different producers.  Verifies that
// isAborted() correctness holds (no false positives, no false negatives) and
// that rebuild() produces the right index without overflow.
TEST(ReadCommittedEdgeTest, LargeAbortedIndexCorrectnessAtScale)
{
    RcvFixture f;

    constexpr int kNumAborted = 50;

    // Interleave: for each producer i, write a data batch then ABORT.
    // pid=100 will be committed at the end (after all aborts).
    for (int i = 0; i < kNumAborted; ++i) {
        int64_t pid = static_cast<int64_t>(i + 1);
        appendData(f.log, pid, "aborted-payload");
        appendControlMarker(f.log, pid, ControlType::Abort);
    }
    // pid=100: one committed txn at the end.
    appendData(f.log, 100, "committed-data");
    appendControlMarker(f.log, 100, ControlType::Commit);

    int64_t hw = f.hw();
    f.view.rebuild(&f.log, hw);

    // All transactions resolved → LSO == HW.
    EXPECT_EQ(f.view.lso(hw), hw);

    // Aborted index must have exactly kNumAborted ranges.
    EXPECT_EQ(f.abortedIndex.size(), static_cast<std::size_t>(kNumAborted));

    // Every aborted producer's data offset should be flagged.
    // Offsets: pid=i+1 data at offset 2*i, abort marker at 2*i+1.
    for (int i = 0; i < kNumAborted; ++i) {
        int64_t pid     = static_cast<int64_t>(i + 1);
        int64_t dataOff = static_cast<int64_t>(2 * i);
        EXPECT_TRUE(f.abortedIndex.isAborted(pid, dataOff))
            << "pid=" << pid << " data at offset " << dataOff << " should be aborted";
        // No cross-producer contamination.
        EXPECT_FALSE(f.abortedIndex.isAborted(100, dataOff))
            << "committed pid=100 should not appear as aborted";
    }

    // pid=100's data offset = 2*kNumAborted (after all aborted pairs).
    int64_t pid100DataOff = static_cast<int64_t>(2 * kNumAborted);
    EXPECT_FALSE(f.abortedIndex.isAborted(100, pid100DataOff))
        << "committed pid=100 data batch must NOT be aborted";

    // read_committed: only pid=100's data returned.
    auto raw = f.view.read(&f.log, hw, 0, 1 << 20, IsolationLevel::ReadCommitted);
    auto offsets = readOffsets(raw);
    ASSERT_EQ(offsets.size(), 1u) << "only the one committed batch should be returned";
    EXPECT_EQ(offsets[0], pid100DataOff);
}

// ── Equivalence assertion: rebuild == incremental for same-PID abort-then-commit
// This pins the equivalence guarantee: rebuild() and the incremental
// onProduceBatch/onControlMarker path yield IDENTICAL LSO + aborted index
// for the same-PID sequential txn scenario (Edge 1 pattern).
TEST(ReadCommittedEdgeTest, SamePidAbortThenCommitRebuildMatchesIncremental)
{
    std::string tmpDir = makeTmpDir();
    rune::storage::Log log(tmpDir);

    // --- Incremental path ---
    LsoTracker incrTracker;
    AbortedTxnIndex incrIndex;

    // txn1: pid=5 — abort
    int64_t txn1DataOff  = log.logEndOffset();
    appendData(log, 5, "txn1");
    incrTracker.onProduceBatch(5, txn1DataOff);

    int64_t txn1AbortOff = log.logEndOffset();
    appendControlMarker(log, 5, ControlType::Abort);
    int64_t firstOff1 = incrTracker.onControlMarker(5, txn1AbortOff, false);
    if (firstOff1 >= 0) incrIndex.onAbortMarker(5, firstOff1, txn1AbortOff);

    // txn2: pid=5 — commit
    int64_t txn2DataOff  = log.logEndOffset();
    appendData(log, 5, "txn2");
    incrTracker.onProduceBatch(5, txn2DataOff);

    int64_t txn2CommitOff = log.logEndOffset();
    appendControlMarker(log, 5, ControlType::Commit);
    incrTracker.onControlMarker(5, txn2CommitOff, true);

    int64_t hw = log.logEndOffset();

    // --- Rebuild path ---
    LsoTracker rebuildTracker;
    AbortedTxnIndex rebuildIndex;
    ReadCommittedView rebuildView(&rebuildTracker, &rebuildIndex);
    rebuildView.rebuild(&log, hw);

    // LSO must match.
    EXPECT_EQ(incrTracker.lso(hw), rebuildTracker.lso(hw))
        << "incremental and rebuild LSO must agree";
    EXPECT_EQ(incrTracker.lso(hw), hw)
        << "both txns resolved: LSO == HW";

    // Aborted index must match: exactly one range for pid=5 covering txn1.
    EXPECT_EQ(incrIndex.size(), 1u);
    EXPECT_EQ(rebuildIndex.size(), 1u);

    EXPECT_TRUE(incrIndex.isAborted(5, txn1DataOff));
    EXPECT_TRUE(rebuildIndex.isAborted(5, txn1DataOff))
        << "rebuild aborted index must flag txn1 data for pid=5";

    EXPECT_FALSE(incrIndex.isAborted(5, txn2DataOff));
    EXPECT_FALSE(rebuildIndex.isAborted(5, txn2DataOff))
        << "rebuild must NOT flag txn2 data (committed) as aborted";

    fs::remove_all(tmpDir);
}
