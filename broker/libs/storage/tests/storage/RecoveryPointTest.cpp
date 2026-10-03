// E1d — durable recovery point (Kafka recovery-point parity).
//
// Production seals segments asynchronously (Log::setDeferSealSync: the owning
// worker fdatasyncs a sealed segment later through its io backend).  On a power
// loss before that fsync completes, recovery used to take the LEO from the LAST
// segment — an empty / short later segment made the log claim offsets it does
// not hold.  These tests pin the recovery-point contract:
//   * R advances IN ORDER (a later segment's durability never skips an earlier,
//     still-pending seal);
//   * recovery trusts segments ending at or below the persisted R (compaction
//     gaps there are fine) and chain-validates everything above it, truncating
//     at the first gap / torn tail and deleting every later segment;
//   * the cleaner never compacts a segment ending above the persisted R.
// Power loss is simulated deterministically: each .log is truncated to the
// bytes the log reported durable (Log::syncedExtentsForTest), after a CRASH
// (Log::crashForTest — no clean-shutdown checkpoint).  No sleeps, no threads.
#include <gtest/gtest.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogCleaner.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

using namespace rune::storage;
using namespace rune::protocol;
namespace fs = std::filesystem;

namespace {

fs::path freshDir(const char* stem) {
    static int seq = 0;
    fs::path d = fs::temp_directory_path() /
                 (std::string(stem) + "_" + std::to_string(::getpid()) + "_" + std::to_string(++seq));
    fs::remove_all(d);
    fs::create_directories(d);
    return d;
}

std::vector<uint8_t> batchAt(int64_t off, const std::string& key = "k") {
    RecordBatchBuilder b(off, 1000 + off);
    b.append(key, "value-" + std::to_string(off), 1000 + off);
    return b.build();
}

// Page-cache append of one single-record batch at the log end.
void appendOne(Log& log, const std::string& key = "k") {
    const int64_t off = log.logEndOffset();
    const auto b = batchAt(off, key);
    log.appendNoSync(b.data(), b.size(), off);
}

// Every offset in [log start, LEO) is readable, in order, without a hole.
void expectGapFree(Log& log) {
    int64_t expect = log.logStartOffset();
    while (expect < log.logEndOffset()) {
        const auto bytes = log.read(expect, 1 << 20);
        ASSERT_FALSE(bytes.empty()) << "hole at offset " << expect << " (LEO " << log.logEndOffset() << ")";
        RecordBatchReader r(bytes.data(), bytes.size());
        ASSERT_TRUE(r.ensureValid());
        ASSERT_EQ(r.baseOffset(), expect) << "offset gap before " << r.baseOffset();
        expect = r.baseOffset() + r.lastOffsetDelta() + 1;
    }
    EXPECT_EQ(expect, log.logEndOffset());
}

// Complete the oldest pending async seal exactly as the worker does: fdatasync
// its fd (here inline), then report the completion.
void completeFrontSeal(Log& log) {
    LogSegment* seg = log.frontPendingSeal();
    ASSERT_NE(seg, nullptr);
    log.popPendingSeal();
    ASSERT_EQ(::fsync(seg->logFd()), 0);
    log.noteFlushCompleted(seg, INT64_MAX);
    log.unpinSegmentFlush(seg);
}

// Async active-segment flush as the worker does it (pin, fdatasync, complete).
void flushActive(Log& log) {
    LogSegment* seg = log.activeSegmentForFlush();
    seg->pinFlush();
    const int64_t boundary = log.flushBoundary();
    ASSERT_EQ(::fsync(seg->logFd()), 0);
    log.noteFlushCompleted(seg, boundary);
    log.unpinSegmentFlush(seg);
}

// Drive one async checkpoint to completion exactly as the worker does (each
// fdatasync is "submitted" then completed; here performed inline).
void driveCheckpoint(Log& log) {
    ASSERT_TRUE(log.beginRecoveryCheckpoint());
    for (int stage = 0; stage < 2; ++stage) {
        const int fd = log.recoveryCheckpointFdToSync();
        ASSERT_GE(fd, 0);
        const uint64_t seq = log.recoveryCheckpointSubmitted();
        log.recoveryCheckpointSyncCompleted(seq, ::fsync(fd) == 0 ? 0 : errno);
    }
    ASSERT_EQ(log.recoveryCheckpointStage(), Log::RpStage::Idle);
}

// CRASH + POWER LOSS: destroy without a clean-shutdown checkpoint, then cut
// every .log to its reported durable extent (-1 = nothing durable).
void crashWithPowerLoss(std::unique_ptr<Log>& log) {
    const auto ext = log->syncedExtentsForTest();
    log->crashForTest();
    log.reset();
    for (const auto& e : ext) {
        if (!fs::exists(e.logPath)) continue;
        const auto size = static_cast<int64_t>(fs::file_size(e.logPath));
        fs::resize_file(e.logPath, static_cast<uintmax_t>(std::min<int64_t>(size, std::max<int64_t>(0, e.syncedBytes))));
    }
}

// CRASH only (process kill): the page cache survives.
void crashKeepPageCache(std::unique_ptr<Log>& log) {
    log->crashForTest();
    log.reset();
}

std::unique_ptr<Log> openRecovered(const fs::path& dir, int64_t roll, bool deferSeals) {
    auto log = std::make_unique<Log>(dir.string(), roll);
    log->recover();
    log->setDeferSealSync(deferSeals);
    return log;
}

int64_t readPersistedR(const fs::path& dir) { return Log::readRecoveryPointFile(dir.string()); }

void writeRFile(const fs::path& dir, int64_t r) {
    std::ofstream f(dir / Log::kRecoveryPointFile, std::ios::trunc);
    f << "1\n" << r << "\n";
}

// Rewrite a segment's .log WITHOUT its last batch (what compaction does when
// the segment's last key is superseded later): an offset gap to the next base.
void dropLastBatch(const std::string& logPath) {
    std::ifstream in(logPath, std::ios::binary);
    std::vector<uint8_t> buf((std::istreambuf_iterator<char>(in)), {});
    in.close();
    std::size_t pos = 0, lastStart = 0;
    while (pos + static_cast<std::size_t>(batch::kRecordBatchOverhead) <= buf.size()) {
        RecordBatchReader r(buf.data() + pos, buf.size() - pos);
        lastStart = pos;
        pos += static_cast<std::size_t>(r.totalLength());
    }
    fs::resize_file(logPath, lastStart);
}

std::string segPath(const fs::path& dir, int64_t base) {
    std::string s = std::to_string(base);
    return (dir / (std::string(20 - s.size(), '0') + s + ".log")).string();
}

constexpr int64_t kRoll = 512;   // a few single-record batches per segment

} // namespace

// ---------------------------------------------------------------------------
// The E1d bug at the storage layer: async seals of the sealed segments never
// completed, the ACTIVE segment was flushed (it is durable).  A power loss
// leaves an empty middle segment; recovery must NOT report the last segment's
// end as the LEO over the missing offsets.
// ---------------------------------------------------------------------------
TEST(RecoveryPointRegression, EmptyLaterSegment_TruncatesAtFirstGap_DeletesLaterSegments) {
    const auto dir = freshDir("rp_empty_later");
    auto log = openRecovered(dir, kRoll, /*deferSeals=*/true);
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log);
    for (int i = 0; i < 3; ++i) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    ASSERT_GE(bases.size(), 4u);
    // seg0's seal completes and R is checkpointed at seg1's base.
    completeFrontSeal(*log);
    driveCheckpoint(*log);
    EXPECT_EQ(log->persistedRecoveryPoint(), bases[1]);
    // seg1's seal never completes; seg2's does (out of order) and the active
    // segment is flushed: R must NOT skip the pending seal of seg1.
    LogSegment* seg1 = log->frontPendingSeal();
    log->popPendingSeal();   // seg1: submitted, never completes
    completeFrontSeal(*log); // seg2
    flushActive(*log);
    EXPECT_EQ(log->recoveryPoint(), bases[1]) << "R advanced past a still-pending seal";
    (void)seg1;

    crashWithPowerLoss(log);   // seg1 has no durable byte: empty after the power loss
    ASSERT_EQ(fs::file_size(segPath(dir, bases[1])), 0u);

    auto rec = openRecovered(dir, kRoll, true);
    EXPECT_EQ(rec->logEndOffset(), bases[1]) << "the log claims offsets it does not hold";
    EXPECT_EQ(rec->segmentBaseOffsets(), (std::vector<int64_t>{bases[0], bases[1]}));
    EXPECT_FALSE(fs::exists(segPath(dir, bases[2])));
    EXPECT_FALSE(fs::exists(segPath(dir, bases[3])));
    expectGapFree(*rec);
    EXPECT_EQ(readPersistedR(dir), bases[1]);
    EXPECT_EQ(rec->recoveryPoint(), bases[1]);
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// Torn tail in a segment ABOVE R (its seal never completed; a few bytes of its
// last batch made it to disk) while the next segment is intact: truncate at the
// last whole batch and delete everything after.
// ---------------------------------------------------------------------------
TEST(RecoveryPointRegression, TornTailAfterRecoveryPoint_TruncatesThereAndDeletesLater) {
    const auto dir = freshDir("rp_torn_tail");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 3) appendOne(*log);
    appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log);
    driveCheckpoint(*log);
    ASSERT_EQ(log->persistedRecoveryPoint(), bases[1]);
    crashKeepPageCache(log);
    // Tear seg1's last batch (power loss mid-writeback); seg2 survived whole.
    const std::string p1 = segPath(dir, bases[1]);
    fs::resize_file(p1, fs::file_size(p1) - 7);

    auto rec = openRecovered(dir, kRoll, true);
    EXPECT_LT(rec->logEndOffset(), bases[2]);
    EXPECT_EQ(rec->logEndOffset(), bases[2] - 1) << "cut at the last whole batch of seg1";
    EXPECT_EQ(rec->segmentBaseOffsets(), (std::vector<int64_t>{bases[0], bases[1]}));
    EXPECT_FALSE(fs::exists(segPath(dir, bases[2])));
    expectGapFree(*rec);
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// Compaction legitimately removes a sealed segment's last record (an offset gap
// to the next segment).  Below R that gap must NOT truncate anything.
// ---------------------------------------------------------------------------
TEST(RecoveryPointRegression, CompactionGapBelowRecoveryPoint_NotTruncated) {
    const auto dir = freshDir("rp_compaction_gap");
    auto log = openRecovered(dir, kRoll, /*deferSeals=*/false);   // inline seals: R persisted per roll
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log);
    appendOne(*log);
    log->sync();
    const auto bases = log->segmentBaseOffsets();
    const int64_t leo = log->logEndOffset();
    ASSERT_GE(log->persistedRecoveryPoint(), bases[3]);
    log.reset();   // clean shutdown: R = LEO persisted
    ASSERT_EQ(readPersistedR(dir), leo);
    dropLastBatch(segPath(dir, bases[0]));   // the "compacted" gap [bases[1]-1]
    dropLastBatch(segPath(dir, bases[2]));

    auto rec = openRecovered(dir, kRoll, false);
    EXPECT_EQ(rec->logEndOffset(), leo) << "a compaction gap below R truncated the log";
    EXPECT_EQ(rec->segmentBaseOffsets(), bases);
    EXPECT_EQ(rec->segmentNextOffsetAt(bases[0]), bases[1] - 1);
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// A stale (LOWER) persisted R only makes recovery validate more: a gap-free log
// recovers completely.
// ---------------------------------------------------------------------------
TEST(RecoveryPointRegression, StaleLowerRecoveryPoint_RecoversCompletely) {
    const auto dir = freshDir("rp_stale_lower");
    auto log = openRecovered(dir, kRoll, false);
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log);
    appendOne(*log);
    log->sync();
    const auto bases = log->segmentBaseOffsets();
    const int64_t leo = log->logEndOffset();
    log.reset();
    writeRFile(dir, 0);   // stale: far below every segment end

    auto rec = openRecovered(dir, kRoll, false);
    EXPECT_EQ(rec->logEndOffset(), leo);
    EXPECT_EQ(rec->segmentBaseOffsets(), bases);
    expectGapFree(*rec);
    EXPECT_EQ(readPersistedR(dir), leo) << "recovery persists R = the recovered LEO";
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// Missing R file (upgrade from a pre-E1d broker / first boot): R = the last
// sealed segment's end — every sealed segment is trusted (a compaction gap
// there survives); only the active segment is validated.
// ---------------------------------------------------------------------------
TEST(RecoveryPointRegression, MissingRecoveryPointFile_TrustsSealedSegments) {
    const auto dir = freshDir("rp_missing");
    auto log = openRecovered(dir, kRoll, false);
    while (log->segmentBaseOffsets().size() < 3) appendOne(*log);
    appendOne(*log);
    log->sync();
    const auto bases = log->segmentBaseOffsets();
    const int64_t leo = log->logEndOffset();
    log.reset();
    fs::remove(dir / Log::kRecoveryPointFile);
    dropLastBatch(segPath(dir, bases[0]));   // compaction gap in a sealed segment

    auto rec = openRecovered(dir, kRoll, false);
    EXPECT_EQ(rec->logEndOffset(), leo);
    EXPECT_EQ(rec->segmentBaseOffsets(), bases);
    EXPECT_EQ(readPersistedR(dir), leo) << "the missing file is written after recovery";
    fs::remove_all(dir);
}

// A brand-new (empty) log writes R = 0 at its first recovery.
TEST(RecoveryPointRegression, FirstBoot_PersistsZero) {
    const auto dir = freshDir("rp_first_boot");
    auto log = openRecovered(dir, kRoll, true);
    EXPECT_EQ(log->logEndOffset(), 0);
    EXPECT_EQ(readPersistedR(dir), 0);
    EXPECT_EQ(log->persistedRecoveryPoint(), 0);
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// Crash between the data fdatasync (seals completed — R advanced in memory)
// and the R persist: the on-disk R is stale-lower, recovery validates the
// (durable, gap-free) segments and keeps all of them.
// ---------------------------------------------------------------------------
TEST(RecoveryPointRegression, CrashBetweenDataFdatasyncAndRPersist_KeepsDurableData) {
    const auto dir = freshDir("rp_crash_before_persist");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 3) appendOne(*log);
    appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log);
    completeFrontSeal(*log);
    flushActive(*log);
    const int64_t leo = log->logEndOffset();
    EXPECT_EQ(log->recoveryPoint(), leo);
    EXPECT_TRUE(log->recoveryCheckpointDue());
    // The checkpoint got as far as its tmp file, then the crash.
    ASSERT_TRUE(log->beginRecoveryCheckpoint());
    EXPECT_EQ(readPersistedR(dir), 0);
    crashWithPowerLoss(log);

    auto rec = openRecovered(dir, kRoll, true);
    EXPECT_EQ(rec->logEndOffset(), leo) << "durable records lost";
    EXPECT_EQ(rec->segmentBaseOffsets(), bases);
    expectGapFree(*rec);
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// Invariant 3: the cleaner compacts only segments ending at or below the
// PERSISTED R — never one recovery will chain-validate.
// ---------------------------------------------------------------------------
TEST(RecoveryPointRegression, CleanerNeverCompactsAboveRecoveryPoint) {
    const auto dir = freshDir("rp_cleaner_gate");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log, "same-key");
    appendOne(*log, "same-key");
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log);            // seg0 durable -> R = bases[1]
    driveCheckpoint(*log);
    ASSERT_EQ(log->persistedRecoveryPoint(), bases[1]);
    completeFrontSeal(*log);            // seg1 durable in memory, NOT persisted
    EXPECT_EQ(log->recoveryPoint(), bases[2]);

    const auto snap = log->sealedSegmentSnapshot();
    ASSERT_EQ(snap->size(), bases.size() - 1);
    EXPECT_TRUE((*snap)[0].belowRecoveryPoint);
    EXPECT_FALSE((*snap)[1].belowRecoveryPoint) << "seg1 ends above the persisted R";
    EXPECT_FALSE((*snap)[2].belowRecoveryPoint);

    const auto before1 = fs::file_size(segPath(dir, bases[1]));
    LogCleaner cleaner;
    EXPECT_FALSE(cleaner.cleanSegment(*log, bases[1], bases[2], 0, 0, 0))
        << "compacted a segment above the persisted recovery point";
    EXPECT_EQ(fs::file_size(segPath(dir, bases[1])), before1);
    EXPECT_TRUE(cleaner.cleanSegment(*log, bases[0], bases[1], 0, 0, 0))
        << "a segment below R must stay compactable";
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// A truncation below an IN-FLIGHT async checkpoint: the checkpoint is
// abandoned (its completion never renames) and the lowered R is persisted
// inline — the on-disk R never exceeds the LEO over re-appended bytes.
// ---------------------------------------------------------------------------
TEST(RecoveryPointRegression, TruncateBelowInFlightCheckpoint_NeverRaisesPersistedR) {
    const auto dir = freshDir("rp_truncate_inflight");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 3) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log);
    completeFrontSeal(*log);
    const int64_t r = log->recoveryPoint();
    ASSERT_EQ(r, bases[2]);
    ASSERT_TRUE(log->beginRecoveryCheckpoint());
    const int tmpFd = log->recoveryCheckpointFdToSync();
    const uint64_t seq = log->recoveryCheckpointSubmitted();   // tmp fsync in flight
    const int64_t cut = bases[1] + 1;
    log->truncateTo(cut);
    EXPECT_EQ(log->recoveryPoint(), log->logEndOffset());
    EXPECT_EQ(readPersistedR(dir), log->logEndOffset()) << "truncation below R must persist the lower R";
    // The superseded checkpoint completes: it must not rename its stale value.
    log->recoveryCheckpointSyncCompleted(seq, ::fsync(tmpFd) == 0 ? 0 : errno);
    EXPECT_EQ(log->recoveryCheckpointStage(), Log::RpStage::Idle);
    EXPECT_EQ(readPersistedR(dir), log->logEndOffset());
    EXPECT_EQ(log->persistedRecoveryPoint(), log->logEndOffset());
    EXPECT_FALSE(log->storageFailed());
    fs::remove_all(dir);
}

// A failing fsync of the recovery-point checkpoint is fail-stop (fsyncgate).
TEST(RecoveryPointRegression, CheckpointFsyncFault_IsFailStop) {
    const auto dir = freshDir("rp_fsync_fault");
    auto log = openRecovered(dir, kRoll, false);
    log->setSyncFaultForTest(EIO);
    EXPECT_NE(log->checkpointRecoveryPoint(), 0);
    EXPECT_TRUE(log->storageFailed());
    log->setSyncFaultForTest(0);
    fs::remove_all(dir);
}

// An async checkpoint completion carrying an error is fail-stop, and the
// on-disk R is unchanged.
TEST(RecoveryPointRegression, AsyncCheckpointError_IsFailStop_NoRename) {
    const auto dir = freshDir("rp_async_err");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 2) appendOne(*log);
    completeFrontSeal(*log);
    ASSERT_TRUE(log->beginRecoveryCheckpoint());
    const uint64_t seq = log->recoveryCheckpointSubmitted();
    log->recoveryCheckpointSyncCompleted(seq, EIO);
    EXPECT_TRUE(log->storageFailed());
    EXPECT_EQ(readPersistedR(dir), 0);
    EXPECT_EQ(log->recoveryCheckpointStage(), Log::RpStage::Idle);
    fs::remove_all(dir);
}

// The legacy (inline-seal) path persists R once per roll — never per append.
TEST(RecoveryPointRegression, InlineSealPath_PersistsOncePerRollNotPerAppend) {
    const auto dir = freshDir("rp_inline_rate");
    auto log = openRecovered(dir, kRoll, false);
    const uint64_t base = log->recoveryCheckpointsForTest();
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log);
    const uint64_t rolls = log->segmentBaseOffsets().size() - 1;
    EXPECT_EQ(log->recoveryCheckpointsForTest() - base, rolls);
    EXPECT_EQ(log->persistedRecoveryPoint(), log->segmentBaseOffsets().back());
    fs::remove_all(dir);
}
