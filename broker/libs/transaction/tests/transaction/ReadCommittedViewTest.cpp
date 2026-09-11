// ---------------------------------------------------------------------------
// ReadCommittedViewTest.cpp — Unit tests for P5-4 read-committed isolation.
//
// Tests: LsoTracker, AbortedTxnIndex, ReadCommittedView::rebuild + read.
//
// Strategy:
//   - Log-independent tests for LsoTracker and AbortedTxnIndex (pure in-memory).
//   - Log-based tests build real rune::storage::Log instances in unique tmp dirs,
//     write data batches (BE via RecordBatchBuilder) and control batches (LE via
//     ControlMarker::build), then call ReadCommittedView::rebuild() + read().
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
    std::string dir = "/tmp/rune_rcv_" + std::to_string(::getpid())
                    + "_" + std::to_string(cnt);
    fs::create_directories(dir);
    return dir;
}

/// Build a big-endian data batch with a specific producerId (overrides the
/// RecordBatchBuilder's default -1 at kProducerIdOffset=43 BE).
///
/// The producerId field at bytes [43..50] in the BE batch header.
static std::vector<uint8_t> makeDataBatch(int64_t baseOffset,
                                           int64_t producerId,
                                           std::string_view value = "payload")
{
    rune::protocol::RecordBatchBuilder builder(baseOffset, 0);
    builder.append({}, value, 0);
    auto batch = builder.build();

    // Patch producerId at kProducerIdOffset (43), big-endian.
    constexpr int kPidOff = rune::protocol::batch::kProducerIdOffset; // 43
    for (int i = 0; i < 8; ++i) {
        batch[kPidOff + i] = static_cast<uint8_t>(
            producerId >> ((7 - i) * 8));
    }

    // Recompute CRC (CRC covers [kAttributesOffset..end], stored at kCrcOffset).
    using namespace rune::protocol;
    // The CRC field and its dependencies:
    //   CRC covers bytes [kAttributesOffset..end].
    //   CRC stored at kCrcOffset (17), as big-endian uint32.
    //   We patch producerId which is inside the CRC region — recompute.
    //
    // Use the same crc32c as rune-protocol (defined inline in Crc32c.h).
    // We can use RecordBatchReader::ensureValid() to verify after patching.
    // But we need to actually recompute the CRC. Use RecordBatchBuilder's
    // own logic by rebuilding. But that would reset producerId.
    //
    // Alternative: use the ControlMarker::crc32c (private) — can't access it.
    //
    // Best approach: Patch the CRC manually using the same polynomial.
    // CRC32C polynomial = 0x82F63B78.
    auto crc32c_fn = [](const uint8_t* data, std::size_t len) -> uint32_t {
        static uint32_t tbl[256] = {};
        static bool init = false;
        if (!init) {
            for (uint32_t i = 0; i < 256; ++i) {
                uint32_t c = i;
                for (int j = 0; j < 8; ++j) c = (c >> 1) ^ (0x82F63B78u & -(c & 1u));
                tbl[i] = c;
            }
            init = true;
        }
        uint32_t crc = 0xFFFFFFFFu;
        for (std::size_t i = 0; i < len; ++i)
            crc = (crc >> 8) ^ tbl[(crc ^ data[i]) & 0xFFu];
        return crc ^ 0xFFFFFFFFu;
    };

    constexpr int kCrcOff   = batch::kCrcOffset;       // 17
    constexpr int kAttrOff  = batch::kAttributesOffset; // 21 — CRC starts here

    uint32_t crc = crc32c_fn(batch.data() + kAttrOff,
                               batch.size() - static_cast<std::size_t>(kAttrOff));
    // Store as big-endian.
    batch[kCrcOff + 0] = static_cast<uint8_t>(crc >> 24);
    batch[kCrcOff + 1] = static_cast<uint8_t>(crc >> 16);
    batch[kCrcOff + 2] = static_cast<uint8_t>(crc >>  8);
    batch[kCrcOff + 3] = static_cast<uint8_t>(crc);

    return batch;
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
    // Set the baseOffset in the batch (bytes [0..7] LE int64).
    std::memcpy(batch.data(), &off, 8);
    // Note: ControlMarker::isControlBatch() does a CRC check. We must re-CRC
    // after patching the baseOffset.  But baseOffset is EXCLUDED from the
    // ControlMarker CRC (bytes [17..20] cover [21..end]).  Actually per the
    // ControlMarker header the CRC covers bytes [21..end] (CRC_DATA_START=21).
    // baseOffset is at [0..7] — outside the CRC region. Safe to patch.
    log.append(batch, off); // lastOffset = off
    return off;
}

/// Read all batch base offsets from a view result.
/// This parses the BE data batches returned by ReadCommittedView::read().
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
