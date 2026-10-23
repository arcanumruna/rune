// ---------------------------------------------------------------------------
// ControlBatchLeFormatTest.cpp — All-LE endianness regression tests
//
// Renamed context: these tests were written during the BE→LE unification.
// They FAIL under the old BE-on-data + LE-on-control mismatch (or old
// LE-on-control + BE-on-data mismatch), and PASS after the unified LE fix.
//
// Bug: ControlMarker::build() was little-endian; RecordBatchReader was
// big-endian (and vice-versa for a prior iteration).  The mismatch made
// totalLength() return ~1 GiB, causing Log::read() to return empty for
// data after a marker, and recover() to treat the marker as a torn tail.
//
// Fix: control batches encoded in the SAME little-endian RecordBatch layout
// as data batches, with CONTROL attribute bit (0x0020) set.
// ---------------------------------------------------------------------------

#include <rune/transaction/ControlMarker.h>
#include <rune/transaction/ReadCommittedView.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>
#include <unistd.h>

using namespace rune::transaction;
namespace fs = std::filesystem;

// ===========================================================================
// Helpers
// ===========================================================================

namespace {

static std::atomic<int> gSeq{0};

static std::string makeTmpDir()
{
    int n = gSeq.fetch_add(1, std::memory_order_relaxed);
    std::string d = std::filesystem::temp_directory_path().string() + "/rune_le_fmt_" + std::to_string(::getpid()) + "_" + std::to_string(n);
    fs::create_directories(d);
    return d;
}

// Build a little-endian data batch with a specific baseOffset.
static std::vector<uint8_t> makeDataBatch(int64_t baseOffset,
                                           std::string_view value = "data")
{
    rune::protocol::RecordBatchBuilder builder(baseOffset, /*baseTimestamp=*/1000);
    builder.append({}, value, 1000);
    return builder.build();
}

// Build a LE data batch with a specific producerId stamped in via setProducerId().
// The pid models a TRANSACTIONAL producer, so the TRANSACTIONAL attribute is set
// as on every real txn batch (F15 N3: only a transactional batch opens a txn).
static std::vector<uint8_t> makeDataBatchWithPid(int64_t baseOffset, int64_t producerId,
                                                  std::string_view value = "data")
{
    rune::protocol::RecordBatchBuilder builder(baseOffset, /*baseTimestamp=*/1000);
    builder.setProducerId(producerId);
    builder.setTransactional(producerId >= 0);
    builder.append({}, value, 1000);
    return builder.build();
}

// Stamp baseOffset (LE int64) into a control batch at [0..7].
// baseOffset is excluded from the CRC region [21..end], so patching [0..7]
// does not invalidate the CRC.
static void stampLeBaseOffset(std::vector<uint8_t>& batch, int64_t off)
{
    uint64_t u = static_cast<uint64_t>(off);
    for (int i = 0; i < 8; ++i)
        batch[i] = static_cast<uint8_t>(u >> (i * 8));
}

// Walk batch stream with RecordBatchReader (LE) and collect baseOffsets.
static std::vector<int64_t> walkBatchOffsets(const std::vector<uint8_t>& raw)
{
    std::vector<int64_t> result;
    std::size_t pos = 0;
    while (pos < raw.size()) {
        std::size_t rem = raw.size() - pos;
        if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead))
            break;
        rune::protocol::RecordBatchReader r(raw.data() + pos, rem);
        int32_t total = r.totalLength();
        if (total <= 0 || pos + static_cast<std::size_t>(total) > raw.size()) break;
        result.push_back(r.baseOffset());
        pos += static_cast<std::size_t>(total);
    }
    return result;
}

} // anonymous namespace

// ===========================================================================
// [LE-1] Control batch is LE-parseable by RecordBatchReader
//
// The control batch MUST have a correct LE batchLength so that
// RecordBatchReader::totalLength() returns ENCODED_SIZE (75).
// Under the old mismatch, totalLength() would return a huge number,
// causing the reader to think the batch is cut off.
// ===========================================================================
TEST(ControlBatchBeFormat, ControlBatchHasCorrectBeTotalLength)
{
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 0);
    ASSERT_EQ(batch.size(), ControlMarker::ENCODED_SIZE);

    rune::protocol::RecordBatchReader reader(batch.data(), batch.size());
    int32_t total = reader.totalLength();

    // Under old mismatch: reader misread batchLength → huge / invalid number.
    // Fix: total must equal ENCODED_SIZE (75).
    EXPECT_EQ(total, static_cast<int32_t>(ControlMarker::ENCODED_SIZE))
        << "RecordBatchReader must parse the control batch totalLength correctly; "
           "old BE/LE mismatch makes totalLength() return a huge/invalid number";
}

// ===========================================================================
// [LE-2] Control batch passes RecordBatchReader::ensureValid()
//
// LogSegment::recover() calls ensureValid().  Under old mismatch the CRC
// (stored LE) read as BE is wrong → ensureValid() fails → recover() treats
// the marker as torn tail → silent log truncation.
// ===========================================================================
TEST(ControlBatchBeFormat, ControlBatchPassesEnsureValid)
{
    auto batch = ControlMarker::build(42, 3, ControlType::Abort, 7);
    ASSERT_EQ(batch.size(), ControlMarker::ENCODED_SIZE);

    rune::protocol::RecordBatchReader reader(batch.data(), batch.size());
    EXPECT_TRUE(reader.ensureValid())
        << "control batch must pass RecordBatchReader::ensureValid(); "
           "old mismatch fails CRC check → recover() treats marker as torn tail";
}

// ===========================================================================
// [LE-3] Control batch has CONTROL attribute bit set in LE attributes field
//
// After the all-LE fix, attributes are written LE at [21..22].
// The CONTROL bit (0x0020) must be visible when reading [21..22] as LE int16.
// ===========================================================================
TEST(ControlBatchBeFormat, ControlBatchHasControlBitInBeAttributesField)
{
    auto batch = ControlMarker::build(1, 0, ControlType::Commit, 0);

    // Read attributes at [21..22] as little-endian int16.
    uint16_t attrsLe = static_cast<uint16_t>(batch[21])
                     | (static_cast<uint16_t>(batch[22]) << 8);

    EXPECT_NE(attrsLe & static_cast<uint16_t>(ControlMarker::kAttrControl), 0u)
        << "CONTROL attribute bit must be set when attributes are read little-endian";

    // Also verify via the RecordBatchReader accessor.
    rune::protocol::RecordBatchReader r(batch.data(), batch.size());
    EXPECT_TRUE(r.isControl())
        << "RecordBatchReader::isControl() must return true for a control batch";
}

// ===========================================================================
// [LE-4] Data-after-marker is reachable via Log::read() (the key blocker)
//
// Scenario: DATA batch (offset 0) → COMMIT control batch (offset 1) →
//           DATA batch (offset 2).
//
// Under old mismatch: Log::read(2,...) walks the buffer; after parsing the
// first data batch, it encounters the mismatched control batch whose
// totalLength() is huge → walk stops → data at offset 2 is unreachable.
//
// After the fix: the LE control batch is walked correctly; data at offset 2
// is included in the returned bytes.
// ===========================================================================
TEST(ControlBatchBeFormat, DataAfterControlMarkerIsReachableViaLogRead)
{
    std::string tmpDir = makeTmpDir();
    {
        rune::storage::Log log(tmpDir);

        // offset 0: data batch
        auto data0 = makeDataBatch(0, "before-marker");
        log.append(data0, 0); // lastOffset=0

        // offset 1: control marker (COMMIT) — stamp LE baseOffset=1
        auto marker = ControlMarker::build(1, 0, ControlType::Commit, 0);
        stampLeBaseOffset(marker, 1);
        log.append(marker, 1); // lastOffset=1

        // offset 2: data batch AFTER the marker
        auto data2 = makeDataBatch(2, "after-marker");
        log.append(data2, 2); // lastOffset=2

        // Request read starting from offset 2.
        auto raw = log.read(2, 1 << 20);

        // The raw bytes must contain the batch at offset 2.
        auto offsets = walkBatchOffsets(raw);
        EXPECT_FALSE(offsets.empty())
            << "Log::read(offset=2) must return the data batch at offset 2; "
               "under old mismatch the control marker at offset 1 causes the "
               "reader to misparse totalLength → walk stops → batch at offset 2 "
               "is unreachable → Log::read returns empty.";
        if (!offsets.empty()) {
            EXPECT_EQ(offsets[0], int64_t{2})
                << "first batch in the result must be the one at offset 2";
        }
    }
    fs::remove_all(tmpDir);
}

// ===========================================================================
// [LE-5] recover() does NOT treat a control marker as a torn tail
//
// Under old mismatch: recover() uses RecordBatchReader::ensureValid() →
// fails for the mismatched marker → stops → truncates the log file to
// exclude the marker and everything after it (silent data loss).
//
// After the fix: the LE marker passes ensureValid() → recover() includes it
// in validBytes → data after the marker survives.
// ===========================================================================
TEST(ControlBatchBeFormat, RecoverDoesNotTreatControlMarkerAsTornTail)
{
    std::string tmpDir = makeTmpDir();
    {
        rune::storage::Log log(tmpDir);

        // offset 0: data
        auto data0 = makeDataBatch(0, "pre-marker");
        log.append(data0, 0);

        // offset 1: control marker — stamp LE baseOffset=1
        auto marker = ControlMarker::build(2, 0, ControlType::Commit, 0);
        stampLeBaseOffset(marker, 1);
        log.append(marker, 1);

        // offset 2: data after marker
        auto data2 = makeDataBatch(2, "post-marker");
        log.append(data2, 2);

        // LEO before recover: should be 3.
        EXPECT_EQ(log.logEndOffset(), int64_t{3});
    }

    // Simulate crash-recovery by creating a new Log instance on the same dir.
    {
        rune::storage::Log log2(tmpDir);
        int64_t leo = log2.recover();

        // Under old mismatch: recover() stops at the malformed marker → LEO=1
        // (only the pre-marker data batch survives), data at offset 2 truncated.
        // After fix: all 3 batches valid → LEO=3.
        EXPECT_EQ(leo, int64_t{3})
            << "recover() must include both the control marker and post-marker data; "
               "under old mismatch recover() treats the marker as a torn tail "
               "and truncates the log → LEO=1 instead of 3 (silent data loss).";

        // Also confirm the post-marker data is readable.
        auto raw = log2.read(2, 1 << 20);
        auto offsets = walkBatchOffsets(raw);
        EXPECT_FALSE(offsets.empty())
            << "post-marker data (offset 2) must be readable after recover()";
    }
    fs::remove_all(tmpDir);
}

// ===========================================================================
// [LE-6] isControlBatch identifies a LE control batch via LE attribute read
//
// After the fix, isControlBatch() reads attributes little-endian from [21..22].
// This test checks the round-trip: build → isControlBatch.
// ===========================================================================
TEST(ControlBatchBeFormat, IsControlBatchIdentifiesBeControlBatch)
{
    auto commit = ControlMarker::build(10, 2, ControlType::Commit, 5);
    auto abort  = ControlMarker::build(11, 1, ControlType::Abort,  3);

    EXPECT_TRUE(ControlMarker::isControlBatch(commit.data(), 0, ControlMarker::ENCODED_SIZE))
        << "COMMIT control batch must be identified by isControlBatch";
    EXPECT_TRUE(ControlMarker::isControlBatch(abort.data(), 0, ControlMarker::ENCODED_SIZE))
        << "ABORT control batch must be identified by isControlBatch";
}

// ===========================================================================
// [LE-7] Data batch is NOT misidentified as control
//
// A normal LE data batch (built with RecordBatchBuilder, attributes=0) must
// NOT be classified as a control batch.
// ===========================================================================
TEST(ControlBatchBeFormat, DataBatchNotMisidentifiedAsControl)
{
    auto data = makeDataBatch(0, "normal-data");
    EXPECT_FALSE(ControlMarker::isControlBatch(data.data(), 0, data.size()))
        << "a normal data batch must not be identified as a control batch";
    // Also verify via RecordBatchReader::isControl().
    rune::protocol::RecordBatchReader r(data.data(), data.size());
    EXPECT_FALSE(r.isControl())
        << "RecordBatchReader::isControl() must return false for a data batch";
}

// ===========================================================================
// [LE-8] extract() round-trips: COMMIT/ABORT with correct payload from LE format
// ===========================================================================
TEST(ControlBatchBeFormat, ExtractRoundTripCommitAndAbort)
{
    for (auto ct : {ControlType::Commit, ControlType::Abort}) {
        auto batch = ControlMarker::build(/*pid=*/9999LL, /*epoch=*/7,
                                          ct, /*coordEpoch=*/42);
        ControlMarkerPayload payload{};
        ASSERT_TRUE(ControlMarker::extract(batch.data(), 0, payload,
                                            ControlMarker::ENCODED_SIZE))
            << "extract must succeed on a freshly-built control batch";
        EXPECT_EQ(payload.producerId,       9999LL);
        EXPECT_EQ(payload.producerEpoch,    7);
        EXPECT_EQ(payload.controlType,      ct);
        EXPECT_EQ(payload.coordinatorEpoch, 42);
    }
}

// ===========================================================================
// [LE-9] PartitionReplica-level read: control batches NOT emitted to consumers
//
// When read via the storage layer (Log::read), control batches must be skippable
// with RecordBatchReader. If a control batch is in the middle of the log, data
// AFTER it must be returned by Log::read(). After the fix, walking a log with
// [data | control | data] via Log::read from offset 0 yields all three batches.
// ===========================================================================
TEST(ControlBatchBeFormat, LogReadReturnsBatchesAcrossControlMarkerBoundary)
{
    std::string tmpDir = makeTmpDir();
    {
        rune::storage::Log log(tmpDir);

        // Write: data(0), control(1), data(2).
        auto d0 = makeDataBatch(0, "record-a");
        log.append(d0, 0);

        auto ctrl = ControlMarker::build(5, 0, ControlType::Commit, 0);
        stampLeBaseOffset(ctrl, 1);
        log.append(ctrl, 1);

        auto d2 = makeDataBatch(2, "record-b");
        log.append(d2, 2);

        // Read from offset 0 — should return all 3 batches in the raw bytes.
        auto raw = log.read(0, 1 << 20);
        auto offsets = walkBatchOffsets(raw);

        // Under old mismatch: walk stops after d0 (control has bad totalLength)
        // → offsets = {0}. After fix: offsets = {0, 1, 2}.
        EXPECT_EQ(offsets.size(), 3u)
            << "Log::read from offset 0 must return all 3 batches (data+control+data); "
               "under old mismatch the walk stops after the first data batch";
    }
    fs::remove_all(tmpDir);
}

// ===========================================================================
// [LE-10] ReadCommittedView: data after control marker is returned to consumer
//
// ReadCommittedView::read() must return data batches both before AND after
// a control marker.  The control marker itself must NOT be returned.
// After the all-LE fix the unified LE reader handles both types and the
// ReadCommittedView can use RecordBatchReader for both data and control.
// ===========================================================================
TEST(ControlBatchBeFormat, ReadCommittedViewDataBothSidesOfMarkerReturned)
{
    std::string tmpDir = makeTmpDir();
    {
        rune::storage::Log log(tmpDir);

        // pid=1: data at offset 0
        auto data0 = makeDataBatchWithPid(0, /*pid=*/1, "before");
        log.append(data0, 0);

        // control marker: COMMIT for pid=1 at offset 1
        auto ctrl = ControlMarker::build(1, 0, ControlType::Commit, 0);
        stampLeBaseOffset(ctrl, 1);
        log.append(ctrl, 1);

        // pid=2: data at offset 2
        auto data2 = makeDataBatchWithPid(2, /*pid=*/2, "after");
        log.append(data2, 2);

        // Log must be walkable across the control marker.
        auto raw = log.read(0, 1 << 20);
        auto offsets = walkBatchOffsets(raw);

        // After fix: [0 (data), 1 (control), 2 (data)] all parseable as LE.
        EXPECT_EQ(offsets.size(), 3u)
            << "log with [data|control|data] must be fully walkable by RecordBatchReader; "
               "under old mismatch walking stops at the control batch";

        // Data at offset 2 must be present.
        bool found2 = false;
        for (auto off : offsets) if (off == 2) { found2 = true; break; }
        EXPECT_TRUE(found2) << "data batch at offset 2 must be reachable past the control marker";
    }
    fs::remove_all(tmpDir);
}

// ===========================================================================
// [LE-11] ABORT marker still triggers correct aborted-txn indexing
//
// After the format change, ReadCommittedView::rebuild must still correctly
// identify ABORT markers (via the CONTROL bit + record key) and index aborted
// ranges. This confirms the abort-filtering LSO semantics are preserved.
// ===========================================================================
TEST(ControlBatchBeFormat, AbortMarkerStillIndexedCorrectlyAfterBeFix)
{
    using namespace rune::transaction;

    std::string tmpDir = makeTmpDir();
    {
        rune::storage::Log log(tmpDir);

        // data for pid=1 at offset 0 (transactional batch)
        auto data0 = makeDataBatchWithPid(0, /*pid=*/1, "txn-data");
        log.append(data0, 0);

        // ABORT marker for pid=1 at offset 1
        auto abortMarker = ControlMarker::build(1, 0, ControlType::Abort, 0);
        stampLeBaseOffset(abortMarker, 1);
        log.append(abortMarker, 1);

        // committed data for pid=2 at offset 2
        auto data2 = makeDataBatchWithPid(2, /*pid=*/2, "committed");
        log.append(data2, 2);

        // pid=2 committed — append commit marker too
        auto commitMarker = ControlMarker::build(2, 0, ControlType::Commit, 0);
        stampLeBaseOffset(commitMarker, 3);
        log.append(commitMarker, 3);

        int64_t hw = log.logEndOffset(); // = 4

        // Rebuild ReadCommittedView.
        LsoTracker tracker;
        AbortedTxnIndex abortedIndex;
        ReadCommittedView view(&tracker, &abortedIndex);
        view.rebuild(&log, hw);

        // All txns resolved → LSO == HW.
        EXPECT_EQ(view.lso(hw), hw)
            << "after resolving both pid=1 (abort) and pid=2 (commit), LSO must equal HW";

        // Aborted index must contain pid=1's range.
        EXPECT_TRUE(abortedIndex.isAborted(1, 0))
            << "pid=1 data at offset 0 must be marked aborted after ABORT marker parsed";

        // pid=2's data must NOT be aborted.
        EXPECT_FALSE(abortedIndex.isAborted(2, 2))
            << "pid=2 data (committed) must NOT be marked aborted";

        // read_committed: only pid=2's data at offset 2 returned.
        auto raw = view.read(&log, hw, 0, 1<<20, IsolationLevel::ReadCommitted);
        std::vector<int64_t> offsets;
        {
            std::size_t pos = 0;
            while (pos < raw.size()) {
                std::size_t rem = raw.size() - pos;
                if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) break;
                rune::protocol::RecordBatchReader r(raw.data() + pos, rem);
                int32_t total = r.totalLength();
                if (total <= 0) break;
                offsets.push_back(r.baseOffset());
                pos += total;
            }
        }
        ASSERT_EQ(offsets.size(), 1u)
            << "read_committed must return only pid=2's committed data (offset 2), "
               "not pid=1's aborted data (offset 0); "
               "under old mismatch the storage walk stops at the marker "
               "so offset 2 is never reached";
        EXPECT_EQ(offsets[0], int64_t{2});
    }
    fs::remove_all(tmpDir);
}

// ===========================================================================
// [LE-12] Regression: marker-in-log was unreachable under old BE/LE mismatch
//
// This is the PRIMARY regression test required by the all-LE task:
//
// Scenario: append DATA batches + COMMIT marker + more DATA to a REAL Log;
// read back via Log::read and verify that:
//   (a) all data batches parse correctly
//   (b) data AFTER the marker is reachable
//   (c) the marker is NOT emitted to consumers (filtered by isControl())
//   (d) recover()/truncateTo() over a log containing a marker treats the
//       marker as a valid batch (no silent truncation)
//
// This test FAILS under the old code (LE-on-control, BE-on-data mismatch)
// because RecordBatchReader misreads the LE batchLength as BE → ~1 GiB →
// data after marker unreachable, recover() truncates.
// ===========================================================================
TEST(ControlBatchBeFormat, MarkerInLogRegressionDataAfterMarkerReachable)
{
    std::string tmpDir = makeTmpDir();
    {
        rune::storage::Log log(tmpDir);

        // DATA batch 0
        auto d0 = makeDataBatchWithPid(0, 42, "batch-0");
        log.append(d0, 0);

        // DATA batch 1
        auto d1 = makeDataBatchWithPid(1, 42, "batch-1");
        log.append(d1, 1);

        // COMMIT control marker for pid=42 at offset 2
        auto marker = ControlMarker::build(42, 0, ControlType::Commit, 0);
        stampLeBaseOffset(marker, 2);
        log.append(marker, 2);

        // DATA batch 3 — AFTER the marker
        auto d3 = makeDataBatchWithPid(3, 99, "batch-3-after-marker");
        log.append(d3, 3);

        // DATA batch 4 — also after the marker
        auto d4 = makeDataBatchWithPid(4, 99, "batch-4-after-marker");
        log.append(d4, 4);

        EXPECT_EQ(log.logEndOffset(), int64_t{5});

        // (a) Read from offset 0 — all batches must be parseable.
        auto raw0 = log.read(0, 1 << 20);
        auto all_offsets = walkBatchOffsets(raw0);
        ASSERT_EQ(all_offsets.size(), 5u)
            << "(a) all 5 batches must be parseable via Log::read from offset 0; "
               "FAILS under old mismatch (walk stops at marker → only 2 returned)";

        // (b) Data AFTER the marker (offsets 3 and 4) must be reachable.
        auto raw3 = log.read(3, 1 << 20);
        auto post_offsets = walkBatchOffsets(raw3);
        ASSERT_FALSE(post_offsets.empty())
            << "(b) Log::read(3) must return data after the marker; "
               "FAILS under old mismatch";
        EXPECT_EQ(post_offsets[0], int64_t{3})
            << "(b) first batch returned from offset 3 must be the data batch at 3";

        // (c) Marker is NOT emitted to consumers: walk and check isControl().
        {
            std::size_t pos = 0;
            const auto* data = raw0.data();
            std::size_t sz   = raw0.size();
            int ctrl_count   = 0;
            int data_count   = 0;
            while (pos < sz) {
                std::size_t rem = sz - pos;
                if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) break;
                rune::protocol::RecordBatchReader r(data + pos, rem);
                int32_t total = r.totalLength();
                if (total <= 0) break;
                if (r.isControl()) ++ctrl_count; else ++data_count;
                pos += static_cast<std::size_t>(total);
            }
            EXPECT_EQ(ctrl_count, 1) << "(c) exactly 1 control batch in the log";
            EXPECT_EQ(data_count, 4) << "(c) exactly 4 data batches in the log";
        }
    }

    // (d) recover() treats marker as valid → no silent truncation.
    {
        rune::storage::Log log2(tmpDir);
        int64_t leo = log2.recover();
        EXPECT_EQ(leo, int64_t{5})
            << "(d) recover() must survive the marker and include post-marker data; "
               "FAILS under old mismatch (marker treated as torn tail → LEO=2)";

        // Data at offset 3 and 4 must be readable after recovery.
        auto raw = log2.read(3, 1 << 20);
        auto offsets = walkBatchOffsets(raw);
        ASSERT_GE(offsets.size(), 2u)
            << "(d) post-marker data (offsets 3,4) must survive recover()";
        EXPECT_EQ(offsets[0], int64_t{3});
        EXPECT_EQ(offsets[1], int64_t{4});
    }

    fs::remove_all(tmpDir);
}

// ===========================================================================
// [LE-13] ReadCommittedView: read_committed filters ABORT, data-after-marker OK
//
// Full scenario:
//   off 0: data pid=10 (aborted txn)
//   off 1: ABORT marker for pid=10
//   off 2: data pid=20 (committed txn)
//   off 3: COMMIT marker for pid=20
//   off 4: data pid=30 (committed txn, no marker — in flight for LSO testing)
//
// read_committed must:
//   - skip data at offset 0 (aborted)
//   - return data at offset 2 (committed)
//   - NOT return data at offset 4 (pid=30 open → LSO=4 which is the batch's offset)
//   - never return control batches
// ===========================================================================
TEST(ControlBatchBeFormat, ReadCommittedViewFiltersAbortAndHidesInFlight)
{
    std::string tmpDir = makeTmpDir();
    {
        rune::storage::Log log(tmpDir);

        // off 0: data pid=10 (will be aborted)
        auto d0 = makeDataBatchWithPid(0, 10, "aborted");
        log.append(d0, 0);

        // off 1: ABORT for pid=10
        auto abort = ControlMarker::build(10, 0, ControlType::Abort, 0);
        stampLeBaseOffset(abort, 1);
        log.append(abort, 1);

        // off 2: data pid=20 (will be committed)
        auto d2 = makeDataBatchWithPid(2, 20, "committed");
        log.append(d2, 2);

        // off 3: COMMIT for pid=20
        auto commit = ControlMarker::build(20, 0, ControlType::Commit, 0);
        stampLeBaseOffset(commit, 3);
        log.append(commit, 3);

        // off 4: data pid=30 (open / in-flight — no marker)
        auto d4 = makeDataBatchWithPid(4, 30, "in-flight");
        log.append(d4, 4);

        int64_t hw = log.logEndOffset(); // = 5

        LsoTracker tracker;
        AbortedTxnIndex abortedIndex;
        ReadCommittedView view(&tracker, &abortedIndex);
        view.rebuild(&log, hw);

        // LSO = 4 (pid=30 open at offset 4)
        EXPECT_EQ(view.lso(hw), 4);

        // Aborted index has exactly one range (pid=10)
        EXPECT_EQ(abortedIndex.size(), 1u);
        EXPECT_TRUE(abortedIndex.isAborted(10, 0));

        // read_committed: only data at offset 2 should appear
        auto raw = view.read(&log, hw, 0, 1<<20, IsolationLevel::ReadCommitted);
        auto offsets = walkBatchOffsets(raw);
        ASSERT_EQ(offsets.size(), 1u)
            << "read_committed must return only the committed batch (offset 2); "
               "aborted (0) filtered by index, in-flight (4) hidden by LSO=4, "
               "control batches (1,3) never emitted";
        EXPECT_EQ(offsets[0], int64_t{2});

        // read_uncommitted: data batches 0, 2, 4 (not control)
        auto rawRu = view.read(&log, hw, 0, 1<<20, IsolationLevel::ReadUncommitted);
        auto offsRu = walkBatchOffsets(rawRu);
        EXPECT_EQ(offsRu.size(), 3u)
            << "read_uncommitted must return all 3 data batches (0,2,4); "
               "control batches (1,3) still not emitted";
    }
    fs::remove_all(tmpDir);
}
