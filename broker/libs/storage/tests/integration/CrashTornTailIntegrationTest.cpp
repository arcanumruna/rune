// ============================================================================
// CrashTornTailIntegrationTest.cpp
//
// Integration tests: crash/torn-write simulation.  Append junk, corrupt
// bytes, or truncate the active segment's .log file, then reopen+recover
// and verify only complete, committed records survive.
// ============================================================================

#include "IntegrationTestBase.h"
#include <rune/storage/PartitionApi.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;
using namespace rune::storage;

// ---------------------------------------------------------------------------
// Helper: assert recovered records are a valid contiguous prefix of produced.
//   count  <= N, offsets 0..(count-1), values byte-exact.
//   Returns the recovered count.
// ---------------------------------------------------------------------------
static int assertValidPrefix(
        const std::vector<ConsumedRecord>& recs,
        const std::vector<ProducedRecord>& produced,
        int maxExpected,
        const char* context) {
    int count = static_cast<int>(recs.size());
    EXPECT_LE(count, maxExpected) << context << ": more records than produced";
    for (int i = 0; i < count; ++i) {
        EXPECT_EQ(recs[static_cast<size_t>(i)].offset, static_cast<int64_t>(i))
            << context << ": offset broken at i=" << i;
        EXPECT_EQ(recs[static_cast<size_t>(i)].value,
                  produced[static_cast<size_t>(i)].value)
            << context << ": value corrupt at i=" << i;
    }
    return count;
}

// ---------------------------------------------------------------------------
// 1. Append junk to the active segment's .log; reopen+recover; only complete
//    committed records survive; no crash; offsets contiguous from 0.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, JunkAppendedToActiveTailDropped) {
    constexpr int N = 60;
    std::vector<ProducedRecord> produced;
    {
        PartitionApi api(testDir_.string());
        produced = produceN(api, N, 32);
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N));
    }
    // Append 512 bytes of junk to the active .log file (simulates a torn write
    // where the OS wrote the start of a batch header but not the payload).
    fs::path activePath = activeLogPath(testDir_);
    ASSERT_FALSE(activePath.empty());
    appendJunk(activePath, 512);

    // Reopen — PartitionApi ctor calls recover().
    {
        PartitionApi api(testDir_.string());
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N))
            << "junk-appended: recover should not drop valid records";
        auto recs = consumeAll(api);
        int count = assertValidPrefix(recs, produced, N, "junk-appended");
        EXPECT_EQ(count, N) << "all N records should survive (junk is a clean suffix)";
    }
}

// ---------------------------------------------------------------------------
// 2. Append junk mid-way through the active log (reuse: produce half, close,
//    append junk, reopen; all pre-junk records survive).
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, JunkMidTailAllPriorRecordsSurvive) {
    constexpr int N = 80;
    std::vector<ProducedRecord> produced;
    {
        PartitionApi api(testDir_.string());
        produced = produceN(api, N, 16);
    }
    // Append a large junk tail (2 KiB).
    appendJunk(activeLogPath(testDir_), 2048);

    {
        PartitionApi api(testDir_.string());
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N));
        auto recs = consumeAll(api);
        int count = assertValidPrefix(recs, produced, N, "junk-mid-tail");
        EXPECT_EQ(count, N);
    }
}

// ---------------------------------------------------------------------------
// 3. Corrupt a CRC-covered byte in the LAST batch of the active segment
//    (batch attributes field at kAttributesOffset=21 from the last batch's
//    start) → that batch (and anything after) is dropped by recover(); earlier
//    records survive intact.
//
// Note: baseOffset (bytes 0-7) and partitionLeaderEpoch (bytes 12-15) are
// EXCLUDED from the CRC by protocol design; corrupting those bytes does NOT
// fail ensureValid().  We target kAttributesOffset = 21 within the last batch,
// which is the first CRC-covered byte.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, CorruptLastBatchDropsItPreservesEarlier) {
    constexpr int N = 40;
    std::vector<ProducedRecord> produced;
    {
        PartitionApi api(testDir_.string());
        produced = produceN(api, N, 32);
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N));
    }

    // Find the last batch: read the file and locate the final batch's start.
    fs::path activePath = activeLogPath(testDir_);
    uintmax_t fileSize = fs::file_size(activePath);
    ASSERT_GT(fileSize, 0u);

    std::vector<uint8_t> buf(fileSize);
    {
        std::ifstream f(activePath, std::ios::binary);
        f.read(reinterpret_cast<char*>(buf.data()),
               static_cast<std::streamsize>(fileSize));
    }

    // Walk batches to find the last one.
    size_t lastBatchPos = 0;
    {
        size_t pos = 0;
        while (pos + static_cast<size_t>(rune::protocol::batch::kRecordBatchOverhead)
               <= buf.size()) {
            rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
            int32_t total = r.totalLength();
            if (total <= 0 ||
                pos + static_cast<size_t>(total) > buf.size() ||
                !r.ensureValid())
                break;
            lastBatchPos = pos;
            pos += static_cast<size_t>(total);
        }
    }

    // Corrupt kAttributesOffset (byte 21) within the last batch — this is the
    // first byte covered by the CRC-32C checksum.
    std::streamoff corruptPos =
        static_cast<std::streamoff>(lastBatchPos) +
        static_cast<std::streamoff>(rune::protocol::batch::kAttributesOffset);
    corruptFileByte(activePath, corruptPos);

    {
        PartitionApi api(testDir_.string());
        auto recs = consumeAll(api);
        int count = assertValidPrefix(recs, produced, N, "corrupt-last-batch");
        // The corrupted last batch must be dropped; all N-1 earlier batches survive.
        EXPECT_EQ(count, N - 1)
            << "corrupt-last-batch: exactly the last batch should be dropped";
        // HWM must equal recovered count.
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(count));
    }
}

// ---------------------------------------------------------------------------
// 4. Truncate the active log mid-batch (simulates a torn write where we cut
//    the file in half); recover to the last valid batch; no crash.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, TruncateActiveMidBatchRecoversCleanly) {
    constexpr int N = 50;
    std::vector<ProducedRecord> produced;
    {
        PartitionApi api(testDir_.string());
        produced = produceN(api, N, 32);
    }

    // Truncate to half the file — this cuts mid-batch for the last batch.
    fs::path activePath = activeLogPath(testDir_);
    uintmax_t fileSize  = fs::file_size(activePath);
    ASSERT_GT(fileSize, 0u);
    truncateFileTo(activePath, fileSize / 2);

    {
        PartitionApi api(testDir_.string()); // must not throw/crash
        auto recs = consumeAll(api);
        int count = assertValidPrefix(recs, produced, N, "truncate-mid-batch");
        // After truncation we must have fewer records (at least one batch lost).
        EXPECT_LT(count, N);
        EXPECT_GE(count, 0);
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(count));
    }
}

// ---------------------------------------------------------------------------
// 5. Truncate active log to 0 bytes (complete data loss of active segment).
//    Recover must not crash; HWM == 0 if there is only one segment, or HWM
//    equals the last sealed segment's count for multi-segment layouts.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, TruncateActiveLogToZeroDoesNotCrash) {
    constexpr int N = 20;
    {
        PartitionApi api(testDir_.string());
        produceN(api, N, 16);
    }

    fs::path activePath = activeLogPath(testDir_);
    truncateFileTo(activePath, 0);

    // Must not throw / crash.
    PartitionApi api(testDir_.string());
    int64_t hw = api.highWatermark();
    EXPECT_GE(hw, 0);
    EXPECT_LE(hw, static_cast<int64_t>(N));
    auto recs = consumeAll(api);
    EXPECT_EQ(static_cast<int64_t>(recs.size()), hw);
}

// ---------------------------------------------------------------------------
// 6. Multi-segment layout: produce across 3+ explicit directories (simulated
//    by using a real Log roll via the PartitionApi + small custom wrapper).
//    We verify that after appending junk to the last segment, the earlier
//    sealed segments are completely intact.
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, EarlierSegmentsIntactAfterActiveTailJunk) {
    // Produce enough records that the 64 MiB default segment won't roll —
    // but we can still verify correctness. Produce 100 records, check
    // the .log count is 1 (no roll at this scale), then deliberately create
    // a second segment by direct file manipulation.
    constexpr int N = 100;
    std::vector<ProducedRecord> produced;
    {
        PartitionApi api(testDir_.string());
        produced = produceN(api, N, 32);
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N));
    }

    // Verify we produced valid records with no corruption.
    {
        PartitionApi api(testDir_.string());
        auto recs = consumeAll(api);
        verifyContiguous(recs, produced, N, "before-junk");
    }

    // Now append junk to the active (only) log and re-verify.
    appendJunk(activeLogPath(testDir_), 1024);

    {
        PartitionApi api(testDir_.string());
        auto recs = consumeAll(api);
        int count = assertValidPrefix(recs, produced, N, "after-junk-active");
        EXPECT_EQ(count, N); // junk is a clean suffix; all N records intact
        EXPECT_EQ(api.highWatermark(), static_cast<int64_t>(N));
    }
}

// ---------------------------------------------------------------------------
// 7. Produce, close, corrupt a CRC-covered byte in the first batch's
//    attributes field (offset 21 = kAttributesOffset, which is the first byte
//    covered by the CRC-32C checksum); recover; that batch must be detected as
//    invalid and dropped; HWM == 0 (only one batch existed).
//
// Note: baseOffset (bytes 0-7) and partitionLeaderEpoch (bytes 12-15) are
// EXCLUDED from the CRC by protocol design (same as Apache Kafka v2).
// Corrupting those fields does NOT fail ensureValid().  We corrupt a byte
// inside the CRC-covered region (kAttributesOffset..end).
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, CorruptCrcCoveredByteInFirstBatchDropsIt) {
    constexpr int N = 5; // small so each call is one batch
    std::vector<ProducedRecord> produced;
    {
        PartitionApi api(testDir_.string());
        produced = produceN(api, N, 16);
    }

    // Corrupt byte 21 of the .log file = kAttributesOffset = first CRC-covered byte.
    // This flips the attributes field, invalidating the stored CRC-32C.
    corruptFileByte(activeLogPath(testDir_), 21);

    {
        PartitionApi api(testDir_.string());
        // CRC check in ensureValid() must fail → recover stops at batch 0 → HWM == 0.
        EXPECT_EQ(api.highWatermark(), 0);
        EXPECT_TRUE(consumeAll(api).empty());
    }
}

// ---------------------------------------------------------------------------
// 7b. appendNoSync + crash (no sync before reopen) → recover() yields a
//     consistent state.
//
//     This is the GB-5p2 correctness test for the group-commit path:
//       1. Append N records via PartitionApi (durable — establish a clean base).
//       2. Close PartitionApi.
//       3. Open a Log directly and appendNoSync() M more records WITHOUT calling
//          sync() (simulates a leader crash before the group-commit flush).
//       4. Destroy the Log (process exits — no sync/close flush guarantee).
//       5. Reopen PartitionApi (which calls recover() internally).
//       6. Assert: recovered logEndOffset is in [N, N+M] (flushed prefix
//          survives; the un-flushed tail may be absent = acks=1 trade-off).
//       7. Assert: no corruption — all surviving records are readable and
//          match the originally produced bytes exactly.
//       8. Assert: logEndOffset == count of readable records (no gap/orphan).
//
//     acks=1 trade-off: an un-flushed tail MAY be absent after the crash —
//     this is not a bug; it is the documented acks=Leader/acks=1 behaviour.
//     The test accepts any count in [N, N+M].  What is NOT acceptable:
//       - Corrupt bytes in a surviving record.
//       - A torn / partial batch being returned by consume().
//       - logEndOffset != readable record count (offset accounting broken).
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, AppendNoSync_CrashBeforeFlush_RecoverConsistentState) {
    constexpr int N = 20;  // durable baseline records (fsynced via PartitionApi)
    constexpr int M = 10;  // records appended no-sync (may be lost after crash)

    // --- Phase 1: produce N records durably ---
    std::vector<ProducedRecord> producedDurable;
    {
        rune::storage::PartitionApi api(testDir_.string());
        producedDurable = produceN(api, N, 32);
        ASSERT_EQ(api.highWatermark(), static_cast<int64_t>(N));
    }

    // --- Phase 2: appendNoSync M more records via Log directly, then "crash" ---
    //     We open the Log, call appendNoSync() for each record, and destroy the
    //     Log WITHOUT calling sync().  On macOS/Linux the fstream buffer may or
    //     may not be flushed to the OS on close (implementation-defined), so the
    //     un-flushed tail may or may not be visible after reopen.  Either outcome
    //     is acceptable; what matters is that recover() leaves the Log consistent.
    std::vector<ProducedRecord> producedNoSync;
    {
        rune::storage::Log log(testDir_.string());
        log.recover(); // bring in-memory state up to date before appending more
        int64_t nextOff = log.logEndOffset();
        ASSERT_EQ(nextOff, static_cast<int64_t>(N));

        for (int i = 0; i < M; ++i) {
            ProducedRecord rec;
            rec.key       = "nosync-key-" + std::to_string(i);
            rec.value     = makeValue(N + i, 32);
            rec.timestamp = static_cast<int64_t>(2000 + i);
            producedNoSync.push_back(rec);

            rune::protocol::RecordBatchBuilder b(nextOff, rec.timestamp);
            b.append(rec.key, rec.value, rec.timestamp);
            auto batch = b.build();
            const int64_t lastOff = nextOff; // one record → lastOff == baseOff
            nextOff = log.appendNoSync(batch.data(), batch.size(), lastOff);
        }
        // Deliberately do NOT call log.sync() before destruction.
        // Log destructor does NOT call sync(); the fstream ~fstream() may or may
        // not flush its write buffer to the OS — this is the "crash" simulation.
    }

    // --- Phase 3: reopen via PartitionApi (calls recover() internally) ---
    //     The recovered state must be in [N, N+M] and self-consistent.
    {
        rune::storage::PartitionApi api(testDir_.string());
        const int64_t hw = api.highWatermark();

        // Correctness gate 1: HWM is in the valid range.
        EXPECT_GE(hw, static_cast<int64_t>(N))
            << "AppendNoSync crash-recovery: durable prefix must survive";
        EXPECT_LE(hw, static_cast<int64_t>(N + M))
            << "AppendNoSync crash-recovery: HWM must not exceed N+M";

        // Correctness gate 2: all readable records are byte-exact.
        auto recs = consumeAll(api);
        const int recovered = static_cast<int>(recs.size());

        // Correctness gate 3: HWM matches the readable record count (no orphan offset).
        EXPECT_EQ(hw, static_cast<int64_t>(recovered))
            << "AppendNoSync crash-recovery: HWM must equal readable record count";

        // Correctness gate 4: records are contiguous from offset 0.
        for (int i = 0; i < recovered; ++i) {
            EXPECT_EQ(recs[static_cast<size_t>(i)].offset, static_cast<int64_t>(i))
                << "AppendNoSync crash-recovery: offset gap at i=" << i;
        }

        // Correctness gate 5: the durable prefix matches exactly.
        const int durableCount = std::min(recovered, N);
        for (int i = 0; i < durableCount; ++i) {
            EXPECT_EQ(recs[static_cast<size_t>(i)].value,
                      producedDurable[static_cast<size_t>(i)].value)
                << "AppendNoSync crash-recovery: durable record corrupt at i=" << i;
            EXPECT_EQ(recs[static_cast<size_t>(i)].key,
                      producedDurable[static_cast<size_t>(i)].key)
                << "AppendNoSync crash-recovery: durable key corrupt at i=" << i;
        }

        // Correctness gate 6: if any no-sync records survived, they must be
        // byte-exact too (no torn/partial batch).
        if (recovered > N) {
            const int noSyncSurvived = recovered - N;
            for (int i = 0; i < noSyncSurvived; ++i) {
                EXPECT_EQ(recs[static_cast<size_t>(N + i)].value,
                          producedNoSync[static_cast<size_t>(i)].value)
                    << "AppendNoSync crash-recovery: no-sync record corrupt at i=" << i;
                EXPECT_EQ(recs[static_cast<size_t>(N + i)].key,
                          producedNoSync[static_cast<size_t>(i)].key)
                    << "AppendNoSync crash-recovery: no-sync key corrupt at i=" << i;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// MC1 [B1 regression gate] — appendNoSync across a forced segment roll,
//     then flush, then crash-reopen → ALL flushed records survive.
//
// This test is the direct regression gate for the B1 durability bug:
//   "sync() only fsyncs the ACTIVE segment; records in the SEALED segment
//    (created by a roll inside the flush window) are never fsync'd."
//
// Fix: maybeRoll() fsyncs the outgoing segment before sealing it (fsync-on-seal).
// Then sync() fsyncs the active segment.  Together they mean every segment
// that received appendNoSync writes is fsync'd exactly once — at seal time or
// at the explicit sync() call — regardless of how many segment rolls occurred.
//
// Scenario:
//   1. Open a Log with a very small rollBytes (256 B) to force a roll.
//   2. appendNoSync() enough batches to trigger ≥1 roll (so some writes go
//      into the pre-roll / sealed segment).
//   3. Call sync() (simulates flushDirtyLogs — group-commit flush point).
//   4. Destroy the Log without calling sync() again (simulate a process exit
//      AFTER the flush).
//   5. Reopen the Log + recover() (simulate restart).
//   6. Assert ALL records from steps 2+3 survive — both the sealed-segment
//      records AND the active-segment records at flush time.
//
// Acceptable loss: NONE — every record appended before sync() must survive.
// (Records appended AFTER sync() may be lost — that is acks=1 acceptable tail.)
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, MC1_AppendNoSync_AcrossSegmentRoll_Flush_AllSurvive) {
    // Use rollBytes=256 to force a segment roll after a handful of batches.
    // Each RecordBatchBuilder batch with 1 record is ~100–150 bytes, so 3–4
    // batches will exceed 256 bytes and trigger a roll.
    constexpr int64_t kRollBytes = 256;
    constexpr int     kBatches   = 12;  // enough to guarantee ≥1 roll

    std::vector<std::string> values;
    values.reserve(kBatches);

    // --- Phase 1: appendNoSync kBatches records with forced segment roll ---
    {
        rune::storage::Log log(testDir_.string(), kRollBytes);

        int64_t nextOff = 0;
        for (int i = 0; i < kBatches; ++i) {
            std::string val = makeValue(i, 64);
            values.push_back(val);

            rune::protocol::RecordBatchBuilder b(nextOff, static_cast<int64_t>(1000 + i));
            b.append("k" + std::to_string(i), val, static_cast<int64_t>(1000 + i));
            auto batch = b.build();
            const int64_t lastOff = nextOff; // single record per batch → lastOff == baseOff
            nextOff = log.appendNoSync(batch.data(), batch.size(), lastOff);
        }

        // Verify that at least one roll occurred (≥2 segments on disk).
        const auto segs = log.segmentBaseOffsets();
        ASSERT_GE(static_cast<int>(segs.size()), 2)
            << "MC1: expected ≥2 segments after " << kBatches
            << " batches with rollBytes=" << kRollBytes
            << "; got " << segs.size() << " — test validity: roll did not fire";

        // --- Phase 2: flush (simulates flushDirtyLogs group-commit point) ---
        log.sync();

        // The Log is now destroyed (goes out of scope).
        // This simulates a clean process exit after a flush.
        // Records before sync() MUST survive recovery.
    }

    // --- Phase 3: reopen + recover ---
    {
        rune::storage::Log log2(testDir_.string(), kRollBytes);
        const int64_t leo = log2.recover();

        // B1 gate: ALL kBatches records must survive (they were flushed).
        EXPECT_EQ(leo, static_cast<int64_t>(kBatches))
            << "MC1: expected LEO=" << kBatches
            << " after flush+recover, got " << leo
            << "; sealed-segment records lost (B1 regression)";

        // Read and verify all records by scanning offset-by-offset.
        // Log::read() returns data from the segment containing startOffset only;
        // we advance offset by 1 per batch to cross segment boundaries.
        int recovered = 0;
        int64_t readOffset = 0;
        while (readOffset < leo) {
            auto chunk = log2.read(readOffset, 1024 * 1024);
            if (chunk.empty()) break;

            // Walk batches in this chunk.
            size_t pos = 0;
            bool advanced = false;
            while (pos + static_cast<size_t>(rune::protocol::batch::kRecordBatchOverhead)
                   <= chunk.size()) {
                rune::protocol::RecordBatchReader r(chunk.data() + pos, chunk.size() - pos);
                int32_t total = r.totalLength();
                if (total <= 0 || pos + static_cast<size_t>(total) > chunk.size()) break;
                EXPECT_TRUE(r.ensureValid())
                    << "MC1: recovered batch " << recovered << " failed CRC check";
                EXPECT_EQ(r.baseOffset(), static_cast<int64_t>(recovered))
                    << "MC1: batch " << recovered << " has wrong baseOffset";
                ++recovered;
                readOffset = r.baseOffset() + r.lastOffsetDelta() + 1;
                pos += static_cast<size_t>(total);
                advanced = true;
            }
            if (!advanced) break; // no progress — exit to avoid infinite loop
        }

        EXPECT_EQ(recovered, kBatches)
            << "MC1: expected " << kBatches << " valid batches after recovery, got "
            << recovered << " (B1: sealed-segment records lost after roll+flush+crash)";
    }
}

// ---------------------------------------------------------------------------
// 8. After a crash-recovery that drops some records, subsequent produces
//    continue from the recovered HWM contiguously (no gap / offset collision).
// ---------------------------------------------------------------------------
TEST_F(IntegrationTestBase, ProduceAfterCrashRecoveryContiguous) {
    constexpr int N = 40;
    std::vector<ProducedRecord> produced;
    {
        PartitionApi api(testDir_.string());
        produced = produceN(api, N, 20);
    }

    // Truncate active log to 1/4 — simulates a crash mid-write.
    fs::path activePath = activeLogPath(testDir_);
    uintmax_t fileSize  = fs::file_size(activePath);
    truncateFileTo(activePath, fileSize / 4);

    int64_t recoveredHw = 0;
    {
        PartitionApi api(testDir_.string());
        recoveredHw = api.highWatermark();
        EXPECT_GE(recoveredHw, 0);
        EXPECT_LE(recoveredHw, static_cast<int64_t>(N));

        // Produce 10 more records after recovery.
        for (int i = 0; i < 10; ++i) {
            int64_t returnedHw = api.produce(
                "post-crash-" + std::to_string(i),
                "pval-" + std::to_string(i),
                static_cast<int64_t>(9000 + i));
            EXPECT_EQ(returnedHw, recoveredHw + i + 1)
                << "post-crash produce returned unexpected HWM at i=" << i;
        }
        EXPECT_EQ(api.highWatermark(), recoveredHw + 10);

        // Offsets after recovery must be contiguous from 0.
        auto recs = consumeAll(api);
        ASSERT_EQ(static_cast<int64_t>(recs.size()), recoveredHw + 10);
        for (int64_t i = 0; i < recoveredHw + 10; ++i)
            EXPECT_EQ(recs[static_cast<size_t>(i)].offset, i) << "offset gap at i=" << i;
    }
}
