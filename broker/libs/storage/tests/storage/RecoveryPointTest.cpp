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
#include <sys/stat.h>

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

// The io backend's directory fsync, as the worker drives it (MEDIUM-2):
// prepare -> fsync(dir fd) -> completion.  Returns false when none was due.
bool driveDirSync(Log& log) {
    Log::DirSync d;
    if (!log.prepareDirectorySync(d)) return false;
    log.directorySyncCompleted(d.gen, ::fsync(d.fd) == 0 ? 0 : errno);
    return true;
}

// Complete the oldest pending async seal: fdatasync its fd (here inline), then
// report the completion — WITHOUT the directory fsync the worker would issue.
void completeFrontSealNoDirSync(Log& log) {
    LogSegment* seg = log.frontPendingSeal();
    ASSERT_NE(seg, nullptr);
    log.popPendingSeal();
    ASSERT_EQ(::fsync(seg->logFd()), 0);
    log.noteFlushCompleted(seg, INT64_MAX);
    log.unpinSegmentFlush(seg);
}

// ... exactly as the worker does: plus the directory fsync once it is due.
void completeFrontSeal(Log& log) {
    completeFrontSealNoDirSync(log);
    (void)driveDirSync(log);
}

// Async active-segment flush as the worker does it (pin, fdatasync, complete
// — and the directory fsync once due).
void flushActive(Log& log) {
    LogSegment* seg = log.activeSegmentForFlush();
    seg->pinFlush();
    const int64_t boundary = log.flushBoundary();
    ASSERT_EQ(::fsync(seg->logFd()), 0);
    log.noteFlushCompleted(seg, boundary);
    log.unpinSegmentFlush(seg);
    (void)driveDirSync(log);
}

// The io backend's work for one checkpoint request: pwrite + fdatasync.
int performWrite(const Log::RpWrite& w) {
    if (::pwrite(w.fd, w.data, w.len, static_cast<off_t>(w.offset)) != static_cast<ssize_t>(w.len)) return errno;
    return ::fsync(w.fd) == 0 ? 0 : errno;
}

// Drive one async checkpoint to completion exactly as the worker does
// (prepare -> submit -> the backend writes + syncs -> completion).
void driveCheckpoint(Log& log) {
    Log::RpWrite w;
    ASSERT_TRUE(log.prepareRecoveryCheckpoint(w));
    log.recoveryCheckpointSubmitted();
    log.recoveryCheckpointCompleted(w.seq, performWrite(w));
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

int64_t readPersistedR(const fs::path& dir) { return Log::readRecoveryPointFile(dir.string()).r; }

// Overwrite one slot of an existing checkpoint file in place.
void writeSlot(const fs::path& dir, int slot, const uint8_t* bytes, std::size_t n) {
    std::fstream f(dir / Log::kRecoveryPointFile, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(f.is_open());
    f.seekp(static_cast<std::streamoff>(slot * Log::kRpSlotStride));
    f.write(reinterpret_cast<const char*>(bytes), static_cast<std::streamsize>(n));
}
// Forge a valid slot holding r with a seq above everything written so far.
void writeRFile(const fs::path& dir, int64_t r) {
    const auto cur = Log::readRecoveryPointFile(dir.string());
    uint8_t img[Log::kRpSlotBytes];
    Log::encodeRpSlot(img, cur.seq + 100, r);
    writeSlot(dir, cur.slot < 0 ? 0 : 1 - cur.slot, img, sizeof img);
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
    // The checkpoint was prepared + submitted, then the crash (never written).
    Log::RpWrite w;
    ASSERT_TRUE(log->prepareRecoveryCheckpoint(w));
    log->recoveryCheckpointSubmitted();
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
// A truncation below an IN-FLIGHT async checkpoint: the lowered R is persisted
// inline FIRST to the OTHER slot with a higher seq; the in-flight write still
// lands afterwards — and never wins (its seq is lower) nor moves persistedRp_.
// ---------------------------------------------------------------------------
TEST(RecoveryPointRegression, TruncateWhileCheckpointInFlight_StaleWriteNeverWins) {
    const auto dir = freshDir("rp_truncate_inflight");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 3) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log);
    completeFrontSeal(*log);
    ASSERT_EQ(log->recoveryPoint(), bases[2]);
    Log::RpWrite w;
    ASSERT_TRUE(log->prepareRecoveryCheckpoint(w));
    log->recoveryCheckpointSubmitted();                   // in flight (not yet executed)
    log->truncateTo(bases[1] + 1);
    const int64_t leo = log->logEndOffset();
    EXPECT_EQ(log->recoveryPoint(), leo);
    EXPECT_EQ(readPersistedR(dir), leo) << "truncation below R must persist the lower R first";
    ASSERT_EQ(performWrite(w), 0);                        // the stale in-flight write lands LATE
    log->recoveryCheckpointCompleted(w.seq, 0);
    EXPECT_EQ(log->recoveryCheckpointStage(), Log::RpStage::Idle);
    EXPECT_EQ(readPersistedR(dir), leo) << "the superseded (higher-R, lower-seq) slot won";
    EXPECT_EQ(log->persistedRecoveryPoint(), leo);
    EXPECT_FALSE(log->storageFailed());
    fs::remove_all(dir);
}

// The same, with the checkpoint only PREPARED (not yet submitted): it is
// dropped, and nothing stale can be submitted afterwards.
TEST(RecoveryPointRegression, TruncateWhileCheckpointPrepared_IsDropped) {
    const auto dir = freshDir("rp_truncate_prepared");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 3) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log);
    completeFrontSeal(*log);
    Log::RpWrite w;
    ASSERT_TRUE(log->prepareRecoveryCheckpoint(w));
    log->truncateTo(bases[1] + 1);
    EXPECT_EQ(log->recoveryCheckpointStage(), Log::RpStage::Idle);
    log->recoveryCheckpointSubmitted();                   // a late submit of the dropped one: no-op
    EXPECT_EQ(log->recoveryCheckpointStage(), Log::RpStage::Idle);
    EXPECT_EQ(readPersistedR(dir), log->logEndOffset());
    EXPECT_FALSE(log->recoveryCheckpointDue());
    fs::remove_all(dir);
}

// F6: the lowered R is persisted BEFORE truncateTo deletes / resizes files: if
// that persist fails (fail-stop), no file is touched.
TEST(RecoveryPointRegression, TruncatePersistsLowerRBeforeTouchingFiles) {
    const auto dir = freshDir("rp_truncate_order");
    auto log = openRecovered(dir, kRoll, false);
    while (log->segmentBaseOffsets().size() < 3) appendOne(*log);
    appendOne(*log);
    log->sync();
    const auto bases = log->segmentBaseOffsets();
    ASSERT_GT(readPersistedR(dir), bases[1]);
    const auto sizeBefore = fs::file_size(segPath(dir, bases[1]));
    log->setSyncFaultForTest(EIO);                        // the R persist fails
    log->truncateTo(bases[1] + 1);
    log->setSyncFaultForTest(0);
    EXPECT_TRUE(log->storageFailed());
    EXPECT_TRUE(fs::exists(segPath(dir, bases[2]))) << "a file was deleted before the lower R was durable";
    EXPECT_EQ(fs::file_size(segPath(dir, bases[1])), sizeBefore);
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
TEST(RecoveryPointRegression, AsyncCheckpointError_IsFailStop) {
    const auto dir = freshDir("rp_async_err");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 2) appendOne(*log);
    completeFrontSeal(*log);
    Log::RpWrite w;
    ASSERT_TRUE(log->prepareRecoveryCheckpoint(w));
    log->recoveryCheckpointSubmitted();
    log->recoveryCheckpointCompleted(w.seq, EIO);
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

// ---------------------------------------------------------------------------
// Out-of-order seal completions advance R strictly in order.
// ---------------------------------------------------------------------------
TEST(RecoveryPointRegression, OutOfOrderSealCompletions_AdvanceRInOrder) {
    const auto dir = freshDir("rp_ooo_seals");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    ASSERT_EQ(log->pendingSealCount(), 3u);
    LogSegment* s0 = log->frontPendingSeal(); log->popPendingSeal();
    LogSegment* s1 = log->frontPendingSeal(); log->popPendingSeal();
    LogSegment* s2 = log->frontPendingSeal(); log->popPendingSeal();
    EXPECT_FALSE(log->noteFlushCompleted(s2, INT64_MAX));
    EXPECT_EQ(log->recoveryPoint(), 0) << "seg2's completion skipped pending seg0/seg1";
    EXPECT_FALSE(log->noteFlushCompleted(s1, INT64_MAX));
    EXPECT_EQ(log->recoveryPoint(), 0);
    EXPECT_TRUE(log->noteFlushCompleted(s0, INT64_MAX)) << "the checkpoint (or its directory fsync) becomes due";
    (void)driveDirSync(*log);                             // MEDIUM-2: the rolled entries become durable
    EXPECT_EQ(log->recoveryPoint(), bases[3]) << "all three in order: R reaches the active base";
    for (LogSegment* sg : {s0, s1, s2}) log->unpinSegmentFlush(sg);
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// Two-slot checkpoint file: a torn NEWEST slot falls back to the older (lower,
// safe) R; both slots invalid = fail-stop (F5) and nothing is truncated or
// deleted.
// ---------------------------------------------------------------------------
TEST(RecoveryPointRegression, TornNewestSlot_UsesOlderSlot) {
    const auto dir = freshDir("rp_torn_slot");
    auto log = openRecovered(dir, kRoll, false);
    while (log->segmentBaseOffsets().size() < 3) appendOne(*log);
    appendOne(*log);
    log->sync();
    const auto bases = log->segmentBaseOffsets();
    const int64_t leo = log->logEndOffset();
    log.reset();                                          // clean: R = LEO in the newest slot
    const auto cur = Log::readRecoveryPointFile(dir.string());
    ASSERT_EQ(cur.state, Log::RpFileState::Valid);
    ASSERT_EQ(cur.r, leo);
    const auto older = [&] {   // what the other slot holds
        std::ifstream f(dir / Log::kRecoveryPointFile, std::ios::binary);
        f.seekg(static_cast<std::streamoff>((1 - cur.slot) * Log::kRpSlotStride));
        uint8_t b[Log::kRpSlotBytes]; f.read(reinterpret_cast<char*>(b), sizeof b);
        uint64_t sq = 0; int64_t r = -1;
        EXPECT_TRUE(Log::decodeRpSlot(b, sq, r));
        return r;
    }();
    const uint8_t torn[7] = {0xde, 0xad, 0xbe, 0xef, 0, 1, 2};
    writeSlot(dir, cur.slot, torn, sizeof torn);
    const auto after = Log::readRecoveryPointFile(dir.string());
    EXPECT_EQ(after.state, Log::RpFileState::Valid);
    EXPECT_EQ(after.r, older);
    EXPECT_LE(after.r, leo);
    auto rec = openRecovered(dir, kRoll, false);
    EXPECT_FALSE(rec->storageFailed());
    EXPECT_EQ(rec->logEndOffset(), leo);
    EXPECT_EQ(rec->segmentBaseOffsets(), bases);
    fs::remove_all(dir);
}

TEST(RecoveryPointRegression, BothSlotsCorrupt_FailStop_NothingDeleted) {
    const auto dir = freshDir("rp_corrupt");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 3) appendOne(*log);
    appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    crashKeepPageCache(log);
    fs::resize_file(segPath(dir, bases[1]), 0);           // a gap a validation would cut at
    const uint8_t junk[Log::kRpSlotBytes] = {1, 2, 3};
    writeSlot(dir, 0, junk, sizeof junk);
    writeSlot(dir, 1, junk, sizeof junk);
    EXPECT_EQ(Log::readRecoveryPointFile(dir.string()).state, Log::RpFileState::Corrupt);
    auto rec = openRecovered(dir, kRoll, true);
    EXPECT_TRUE(rec->storageFailed()) << "a corrupt checkpoint must be fail-stop, not 'trust everything'";
    EXPECT_EQ(rec->segmentBaseOffsets(), bases) << "a fail-stopped recovery deleted segments";
    EXPECT_TRUE(fs::exists(segPath(dir, bases[2])));
    fs::remove_all(dir);
}

// The file is created once (pre-allocated, two slots); afterwards a
// checkpoint never creates, renames or resizes it.
TEST(RecoveryPointRegression, CheckpointFile_PreallocatedOnce_NeverRecreated) {
    const auto dir = freshDir("rp_prealloc");
    auto log = openRecovered(dir, kRoll, true);
    const auto p = dir / Log::kRecoveryPointFile;
    ASSERT_TRUE(fs::exists(p));
    EXPECT_EQ(fs::file_size(p), Log::kRpFileBytes);
    struct ::stat st0{}; ASSERT_EQ(::stat(p.c_str(), &st0), 0);
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log);
    for (int i = 0; i < 3; ++i) { completeFrontSeal(*log); driveCheckpoint(*log); }
    struct ::stat st1{}; ASSERT_EQ(::stat(p.c_str(), &st1), 0);
    EXPECT_EQ(st0.st_ino, st1.st_ino) << "the checkpoint file was recreated / renamed";
    EXPECT_EQ(fs::file_size(p), Log::kRpFileBytes);
    EXPECT_FALSE(fs::exists(dir / Log::kRecoveryPointTmpFile));
    EXPECT_EQ(readPersistedR(dir), log->segmentBaseOffsets().back());
    fs::remove_all(dir);
}

// F2: LogCleaner::compact (the append-thread full-log path) stops at the first
// segment ending above the persisted recovery point.
TEST(RecoveryPointRegression, CompactNeverRewritesAboveRecoveryPoint) {
    const auto dir = freshDir("rp_compact_gate");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log, "same-key");
    appendOne(*log, "same-key");
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log);
    driveCheckpoint(*log);                                // persisted R = bases[1]
    completeFrontSeal(*log);                              // in memory only
    ASSERT_EQ(log->persistedRecoveryPoint(), bases[1]);
    const auto size0 = fs::file_size(segPath(dir, bases[0]));
    const auto size1 = fs::file_size(segPath(dir, bases[1]));
    LogCleaner cleaner;
    cleaner.clean(*log, 0, log->logEndOffset(), 0, 0);
    EXPECT_TRUE(!fs::exists(segPath(dir, bases[0])) || fs::file_size(segPath(dir, bases[0])) < size0)
        << "below R must still compact";
    EXPECT_EQ(fs::file_size(segPath(dir, bases[1])), size1) << "compacted a segment above the persisted R";
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// E1d fix round 2, MAJOR-1 (re-review repros): a checkpoint write that LANDED
// durably while the log was unowned (its completion went to the unowned
// worker) is "possibly durable".  The fresh owner's reset must keep it as an
// upper bound of what may be on disk: a later truncation below that bound
// lowers the on-disk R first — never leaving R above the truncated LEO.
// ---------------------------------------------------------------------------
TEST(RecoveryPointRegression, ResetForNewOwnerThenTruncate_DiskRNeverAboveLeo) {
    const auto dir = freshDir("rp_reset_trunc");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 3) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log);
    completeFrontSeal(*log);
    ASSERT_EQ(log->recoveryPoint(), bases[2]);
    const int64_t P = log->persistedRecoveryPoint();
    Log::RpWrite w;
    ASSERT_TRUE(log->prepareRecoveryCheckpoint(w));
    log->recoveryCheckpointSubmitted();
    ASSERT_EQ(performWrite(w), 0);                        // landed; completion unobserved
    log->resetRecoveryCheckpointForNewOwner();            // fresh owner: outcome unknown
    ASSERT_LE(P, bases[1] + 1);
    log->truncateTo(bases[1] + 1);
    const int64_t leo = log->logEndOffset();
    crashKeepPageCache(log);
    EXPECT_LE(readPersistedR(dir), leo) << "on-disk R " << readPersistedR(dir)
        << " is ABOVE the truncated LEO " << leo << " (P=" << P << ")";
    fs::remove_all(dir);
}

// MAJOR-1 end to end: reset + truncate, re-append (divergent data) past the
// landed value with async seals that never complete, power loss -> no gap.
TEST(RecoveryPointRegression, ResetForNewOwnerThenTruncate_PowerLoss_NoOffsetGap) {
    const auto dir = freshDir("rp_reset_gap");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log); completeFrontSeal(*log); completeFrontSeal(*log);
    Log::RpWrite w;
    ASSERT_TRUE(log->prepareRecoveryCheckpoint(w));
    log->recoveryCheckpointSubmitted();
    ASSERT_EQ(performWrite(w), 0);
    log->resetRecoveryCheckpointForNewOwner();
    log->truncateTo(bases[0] + 1);
    flushActive(*log);
    while (log->logEndOffset() < bases[3] + 2) appendOne(*log, "x");
    crashWithPowerLoss(log);
    auto rec = std::make_unique<Log>(dir.string(), kRoll);
    rec->recover();
    expectGapFree(*rec);
    fs::remove_all(dir);
}

// Control for the above: the same sequence with the completion OBSERVED.
TEST(RecoveryPointRegression, CompletionObservedThenTruncate_PowerLoss_NoOffsetGap) {
    const auto dir = freshDir("rp_ctrl_gap");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log); completeFrontSeal(*log); completeFrontSeal(*log);
    Log::RpWrite w;
    ASSERT_TRUE(log->prepareRecoveryCheckpoint(w));
    log->recoveryCheckpointSubmitted();
    ASSERT_EQ(performWrite(w), 0);
    log->recoveryCheckpointCompleted(w.seq, 0);
    log->truncateTo(bases[0] + 1);
    flushActive(*log);
    while (log->logEndOffset() < bases[3] + 2) appendOne(*log, "x");
    crashWithPowerLoss(log);
    auto rec = std::make_unique<Log>(dir.string(), kRoll);
    rec->recover();
    expectGapFree(*rec);
    fs::remove_all(dir);
}

// ---------------------------------------------------------------------------
// E1d fix round 2, MEDIUM-2: R never passes the base of a segment whose
// directory entry is not yet durable.  The seals of A and B completed, but
// B's entry (created at the roll) is not covered by a directory fsync: R
// stops at B's base.  Once the directory fsync completes, R reaches C.
// ---------------------------------------------------------------------------
TEST(RecoveryPointRegression, RNeverPassesSegmentWhoseDirectoryEntryIsNotDurable) {
    const auto dir = freshDir("rp_dir_gate");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 3) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSealNoDirSync(*log);
    completeFrontSealNoDirSync(*log);
    EXPECT_LE(log->recoveryPoint(), bases[1])
        << "R passed segment " << bases[1] << " before its directory entry was durable";
    EXPECT_TRUE(log->directorySyncDue()) << "the blocked R must request ONE directory fsync";
    EXPECT_FALSE(log->recoveryCheckpointDue() && log->recoveryPoint() > bases[1]);
    Log::DirSync d;
    ASSERT_TRUE(log->prepareDirectorySync(d));
    EXPECT_FALSE(log->directorySyncDue()) << "one directory fsync in flight at a time";
    EXPECT_TRUE(log->directorySyncCompleted(d.gen, ::fsync(d.fd) == 0 ? 0 : errno))
        << "the completion makes the checkpoint due";
    EXPECT_EQ(log->recoveryPoint(), bases[2]);
    EXPECT_FALSE(log->directorySyncDue());
    EXPECT_EQ(log->syncCallsForTest(), 0u) << "no inline fsync on the worker-driven path";
    fs::remove_all(dir);
}

// MEDIUM-2 end to end: the checkpoint is written as soon as it is due, then a
// power loss loses B's (never fsynced) directory entry.  B must not be in the
// trusted region: recovery validates the chain from R and never reports a hole.
TEST(RecoveryPointRegression, LostDirectoryEntryNeverBecomesAnOffsetHole) {
    const auto dir = freshDir("rp_dir_lost");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSealNoDirSync(*log);
    completeFrontSealNoDirSync(*log);
    completeFrontSealNoDirSync(*log);
    if (log->recoveryCheckpointDue()) driveCheckpoint(*log);
    crashKeepPageCache(log);
    fs::remove(segPath(dir, bases[1]));                   // B's entry never became durable
    auto rec = std::make_unique<Log>(dir.string(), kRoll);
    rec->recover();
    expectGapFree(*rec);
    fs::remove_all(dir);
}

// A truncation's deletions are directory mutations too: R does not move past
// the truncation point again until a directory fsync covered them.
TEST(RecoveryPointRegression, TruncationDeletionsGateRUntilDirectorySync) {
    const auto dir = freshDir("rp_dir_trunc");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 3) appendOne(*log);
    const auto bases = log->segmentBaseOffsets();
    completeFrontSeal(*log);
    completeFrontSeal(*log);
    log->truncateTo(bases[1] + 1);                        // deletes C
    const int64_t r0 = log->recoveryPoint();
    appendOne(*log, "x"); appendOne(*log, "x");
    LogSegment* seg = log->activeSegmentForFlush();
    seg->pinFlush();
    const int64_t boundary = log->flushBoundary();
    ASSERT_EQ(::fsync(seg->logFd()), 0);
    log->noteFlushCompleted(seg, boundary);
    log->unpinSegmentFlush(seg);
    EXPECT_EQ(log->recoveryPoint(), r0) << "R moved past the truncation before its deletions were durable";
    EXPECT_TRUE(log->directorySyncDue());
    ASSERT_TRUE(driveDirSync(*log));
    EXPECT_EQ(log->recoveryPoint(), boundary);
    fs::remove_all(dir);
}

// The legacy inline-seal path fsyncs the directory inline at the roll, so its
// R (and the once-per-roll checkpoint) still reach the active base.
TEST(RecoveryPointRegression, LegacyRollFsyncsDirectoryInline) {
    const auto dir = freshDir("rp_dir_legacy");
    auto log = openRecovered(dir, kRoll, false);
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log);
    EXPECT_GE(log->directorySyncsCompletedForTest(), 2u) << "no directory fsync for the rolled segments";
    EXPECT_EQ(log->persistedRecoveryPoint(), log->segmentBaseOffsets().back());
    fs::remove_all(dir);
}

// E1d fix round 3, LOW-C: a recovery that deletes segments at a chain gap
// fsyncs the directory exactly ONCE (the deletions and every entry present
// are covered by the same fsync), and directory fsyncs are not counted as
// role-change syncs.
TEST(RecoveryPointRegression, RecoveryWithGapDeletion_FsyncsDirectoryOnce) {
    const auto dir = freshDir("rp_dir_once");
    auto log = openRecovered(dir, kRoll, true);
    while (log->segmentBaseOffsets().size() < 4) appendOne(*log);
    flushActive(*log);
    crashWithPowerLoss(log);                              // seals never completed: a gap above R
    auto rec = std::make_unique<Log>(dir.string(), kRoll);
    rec->recover();
    ASSERT_LT(rec->segmentBaseOffsets().size(), 4u) << "precondition: recovery deleted segments";
    EXPECT_EQ(rec->directorySyncsCompletedForTest(), 1u) << "recovery fsynced the directory more than once";
    fs::remove_all(dir);
}
