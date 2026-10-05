#include <rune/storage/Timestamps.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <rune_fr/FlightRecorder.h>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <new>
#include <fstream>
#include <spdlog/spdlog.h>
#include <cerrno>
#include <system_error>
#include <fcntl.h>
#include <unistd.h>
#include <climits>
#include <cstring>
#include <rune/protocol/Crc32c.h>
namespace rune::storage {

namespace {
// F3 fix-round-2, items B.2/B.3: spdlog's default logger writes to STDOUT,
// not stderr — a fatal halt must also be visible on stderr (where operator
// tooling, `ctest --output-on-failure`, and EXPECT_DEATH's capture all look
// for a crashing process's diagnostic) regardless of spdlog's sink config.
[[noreturn]] void fatalHalt(const std::string& msg) {
    spdlog::critical("{}", msg);
    spdlog::default_logger()->flush();
    std::fprintf(stderr, "FATAL: %s\n", msg.c_str());
    std::fflush(stderr);
    std::abort();
}
} // namespace

namespace {
// CL4: Shared helper to derive the .index path from a .log path.
// Avoids fragile repeated ".substr(0, path.size()-4) + ".index"" surgery.
inline std::string indexPathForLog(const std::string& logPath) {
    return logPath.substr(0, logPath.size() - 4) + ".index";
}
// Default index sampling interval — MUST match LogSegment's default
// indexIntervalBytes so index right-sizing matches the actual sampling rate.
constexpr int32_t kDefaultIndexIntervalBytes = 4096;
// Largest epoch-ms batch maxTimestamp across EVERY batch header in
// [data, data+len) — an append buffer may hold many batches (the follower
// apply path appends a whole fetch chunk at once).  Header-only walk: no
// allocation, no CRC (the caller already validated / is writing the bytes);
// a single-batch append is exactly one iteration.  Starts at 0 like the
// running max it feeds.
inline int64_t maxTimestampOfBatches(const uint8_t* data, std::size_t len) noexcept {
    int64_t maxTs = 0;
    std::size_t pos = 0;
    while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= len) {
        rune::protocol::RecordBatchReader r(data + pos, len - pos);
        const int64_t ts = toEpochMs(r.maxTimestamp());   // ns- or ms-stamped -> epoch ms
        if (ts > maxTs) maxTs = ts;
        const int32_t total = r.totalLength();
        if (total < rune::protocol::batch::kRecordBatchOverhead) break;
        pos += static_cast<std::size_t>(total);
    }
    return maxTs;
}
// Largest epoch-ms batch maxTimestamp of a segment file: a streaming walk over
// the batch HEADERS only (seek past each payload) — O(1) memory, no whole-file
// buffer.  Cold paths only (recover / reloadSegments), after recovery already
// CRC-validated and cut each segment's torn tail.  false = unreadable.
bool fileMaxBatchTimestamp(const std::string& path, int64_t& out) {
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) return false;
    f.seekg(0, std::ios::end);
    const std::streamoff fileSize = f.tellg();
    if (fileSize < 0) return false;
    constexpr std::size_t kHdr = static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
    uint8_t hdr[kHdr];
    int64_t maxTs = 0;
    std::streamoff pos = 0;
    while (pos + static_cast<std::streamoff>(kHdr) <= fileSize) {
        f.seekg(pos, std::ios::beg);
        if (!f.read(reinterpret_cast<char*>(hdr), static_cast<std::streamsize>(kHdr))) break;
        rune::protocol::RecordBatchReader r(hdr, kHdr);
        const int32_t total = r.totalLength();
        if (total < static_cast<int32_t>(kHdr) || pos + total > fileSize) break;
        const int64_t ts = toEpochMs(r.maxTimestamp());
        if (ts > maxTs) maxTs = ts;
        pos += total;
    }
    out = maxTs;
    return true;
}
} // anonymous namespace

int32_t Log::indexMaxBytesFor(int64_t rollBytes, int32_t indexIntervalBytes) noexcept {
    // Entries needed for a full rollBytes segment: one per indexIntervalBytes,
    // plus the first-batch entry, plus generous headroom (2x) for dense-index
    // corner cases (many small batches each crossing the interval).  OffsetIndex
    // grows the mapping on demand anyway, so this is only an initial estimate.
    if (indexIntervalBytes <= 0) indexIntervalBytes = kDefaultIndexIntervalBytes;
    int64_t entries = (rollBytes / indexIntervalBytes) + 2;
    int64_t bytes   = entries * 8 /*kEntrySize*/ * 2 /*headroom*/;
    constexpr int64_t kMin = 16 * 1024;          // floor: tiny segments still get a page or two
    constexpr int64_t kMax = 8 * 1024 * 1024;    // cap: never exceed the old 8 MiB
    if (bytes < kMin) bytes = kMin;
    if (bytes > kMax) bytes = kMax;
    return static_cast<int32_t>(bytes);
}

Log::Log(std::string dir, int64_t segmentRollBytes) : dir_(std::move(dir)), rollBytes_(segmentRollBytes) {
    indexMaxBytes_ = indexMaxBytesFor(rollBytes_, kDefaultIndexIntervalBytes);
    // Reconcile any staging segment files left by a crash BEFORE discovering real
    // segments: an adopted-but-not-yet-renamed staging.log has its true base
    // recovered from the first batch header and is renamed into place (no data
    // loss); un-adopted / empty staging files are removed.
    recoverStagingFiles();
    // Fix round 2: settle compaction swaps interrupted by a crash (before the
    // .log discovery below).
    recoverCleanerSwapFiles();
    // Discover existing segments; else create the first at offset 0.
    std::vector<int64_t> bases;
    for (auto& e : std::filesystem::directory_iterator(dir_)) {
        if (e.path().extension() == ".log")
            bases.push_back(std::stoll(e.path().stem().string()));
        // CP-9 crash-safe: .deleted files from a prior run must be cleaned up.
        // They were already removed from segments_ (rename happened before crash)
        // so we schedule immediate physical deletion by recording them with
        // renamedAtMs=0 (instantly expired on first physicallyRemoveExpiredDeletedFiles call).
        if (e.path().extension() == ".deleted") {
            pendingPhysicalDeletes_.emplace_back(e.path().string(), 0LL);
        }
    }
    std::sort(bases.begin(), bases.end());
    // Fix round 4: drop every segment wholly below the persisted log start — a
    // retention deletion the owner applied whose file rename a crash cut short
    // (never the last segment; a missing / corrupt checkpoint drops nothing).
    persistedLogStart_ = readLogStartCheckpoint(dir_);
    if (persistedLogStart_ > 0 && bases.size() > 1) {
        std::vector<int64_t> kept;
        kept.reserve(bases.size());
        for (std::size_t i = 0; i < bases.size(); ++i) {
            if (i + 1 < bases.size() && bases[i + 1] <= persistedLogStart_) {
                spdlog::warn("Log {}: dropping segment {} below the persisted log start {}",
                             dir_, bases[i], persistedLogStart_);
                std::error_code rec;
                std::filesystem::remove(logPathForBase(bases[i]), rec);
                std::filesystem::remove(indexPathForBase(bases[i]), rec);
            } else {
                kept.push_back(bases[i]);
            }
        }
        bases.swap(kept);
    }
    hadSegmentsAtOpen_ = !bases.empty();
    if (bases.empty()) bases.push_back(0);
    // E1d: precomputed checkpoint paths (no per-roll allocation); the value
    // recovery would read (a non-recovered Log never writes it).
    rpPath_    = dir_ + "/" + kRecoveryPointFile;
    rpTmpPath_ = dir_ + "/" + kRecoveryPointTmpFile;
    {
        const RpFileRead rf = readRecoveryPointFile(dir_);
        if (rf.state == RpFileState::Valid) persistedRp_ = rf.r;
    }
    // F3: eager read-map ONLY the LAST (highest-base) segment — it becomes the
    // active segment.  Earlier (sealed) segments map their .log lazily on first
    // readView(), so N retained segments no longer means N read mmaps + N fds
    // kept alive for the whole process lifetime.
    for (int64_t b : bases)
        install_(b, std::make_unique<LogSegment>(dir_, b, kDefaultIndexIntervalBytes,
                                                  indexMaxBytes_, /*prefaultIndex*/false,
                                                  /*eagerReadMap*/ b == bases.back()));
    logEndOffset_ = activeSegment().nextOffset();
    // #5 non-blocking append: preallocate the active segment's blocks so hot-path
    // O_APPEND pwrite()s never stall on block allocation (append-p99 tail).
    activeSegment().preallocate(rollBytes_);
}
Log::~Log() {
    // Fix round 4: the cleaner thread is stopped before the Log is destroyed —
    // complete every posted file operation (a simulated crash only frees the
    // objects) and free any output still queued for the owner (its .cleaned
    // files are deleted at the next construction).
    // The cleaner is stopped: this thread is now the msg ring's only consumer.
    while (!fileOpsQuiescent()) {
        const std::size_t before = msgHead_.load(std::memory_order_relaxed);
        (void)applyCleanerMsgs_(/*nowMs=*/0, /*durable=*/!crashedForTest_);
        if (msgHead_.load(std::memory_order_relaxed) == before) {   // stuck (checkpoint write failing)
            crashedForTest_ = true;   // free the objects; disk recovers at the next construction
        }
    }
    {
        const std::size_t head = swapHead_.load(std::memory_order_relaxed);
        const std::size_t tail = swapTail_.load(std::memory_order_acquire);
        for (std::size_t i = head; i != tail; ++i) delete swapRing_[i & kSwapRingMask].prebuilt;
    }
    // E1d: clean shutdown persists the exact recovery point (a stale, lower one
    // would only make the next recovery validate more).  Never on a failed log.
    if (recovered_ && !crashedForTest_ && storage_.failedErrno == 0 &&
        (recoveryPoint_ != persistedRp_ || rpUnconfirmedMax_ > recoveryPoint_)) {
        std::error_code ec;
        if (std::filesystem::is_directory(dir_, ec)) (void)checkpointRecoveryPoint();   // dir removed: nothing to persist
    }
    if (rpFd_ >= 0) { ::close(rpFd_); rpFd_ = -1; }
    if (dirFd_ >= 0) { ::close(dirFd_); dirFd_ = -1; }
    // The background thread has been stopped by the owner before Log destruction,
    // so these are single-threaded.  Finalize any pending staging→base renames so
    // a clean shutdown leaves correctly-named files on disk (otherwise the next
    // recover() would reconcile them anyway — this is just hygiene).
    {
        const std::size_t head = renameReqHead_.load(std::memory_order_relaxed);
        const std::size_t tail = renameReqTail_.load(std::memory_order_relaxed);
        for (std::size_t i = head; i != tail; ++i) {
            const auto rr = renameReqRing_[i & kRenameRingMask];
            renameStagingToBase(rr.seq, rr.base);
        }
    }
    // Free any prepared-but-unconsumed staging segment (raw ptr handed via the
    // atomic handoff slot).  Its destructor closes fds / unmaps; then remove its
    // orphaned staging files from disk.
    LogSegment* prepared = preparedSegment_.exchange(nullptr, std::memory_order_acq_rel);
    if (prepared) {
        const int64_t seq = prepared->stagingSeq();
        delete prepared;
        std::error_code ec;
        std::filesystem::remove(stagingLogPathFor(seq), ec);
        std::filesystem::remove(stagingIndexPathFor(seq), ec);
    }
}

LogSegment& Log::activeSegment() {
    // F3 fix-round-2, item B.3: segments_ must never be empty once the Log is
    // constructed; every reconstruction path (ctor, reloadSegments(),
    // truncateTo()) is now written to either fully succeed or leave segments_
    // completely untouched on failure (items B.1/B.2).  This check converts
    // what would otherwise be silent UB (dereferencing rbegin() on an empty
    // map) into a loud, diagnosable halt if that invariant is EVER broken —
    // intentionally a release-mode check (not a debug-only assert()), since
    // this is a correctness invariant, not a performance-sensitive hot path
    // (a single .empty() branch costs nothing measurable either way).
    if (segments_.empty()) [[unlikely]] {
        fatalHalt("Log::activeSegment(): invariant violation — segments_ is empty "
                  "(dir=" + dir_ + ").  A prior reconstruction must have failed to "
                  "maintain an active segment.  Halting rather than risk UB from "
                  "dereferencing an invalid iterator.");
    }
    return *segments_.rbegin()->second;
}

void Log::maybeRoll() {
    // CL6: Roll decision is made BEFORE writing the new batch (Kafka-style).
    // A segment can therefore exceed rollBytes by at most one batch.
    // A single oversized batch (larger than rollBytes) always starts a new
    // segment of its own — no splitting occurs.
    if (activeSegment().sizeBytes() >= rollBytes_) {
        // B1 fix (fsync-on-seal): fsync the segment being sealed BEFORE
        // opening a new one.  appendNoSync() writes to page cache only; if
        // a roll fires mid-flush-window the sealed segment would otherwise
        // never be fsync'd (Log::sync() only fsyncs the ACTIVE segment).
        // Fsyncing here guarantees that every segment receiving a no-sync
        // write is fsync'd exactly once — at seal time — so Log::sync()
        // after a roll means "all no-sync writes since last sync are on disk
        // across all segment boundaries".
        if (deferSealSync_ && pendingSealCount_ < kMaxPendingSeals) {
            // Shard mode (spec §3.1): no inline fsync on the loop.  Push the
            // buffered bytes to the page cache, pin the sealed segment (its fd
            // must outlive the async flush — M1) and queue it; the owning worker
            // fdatasyncs it through its io backend, and Strict acks on this log
            // wait until every pending seal has completed.
            LogSegment& sealed = activeSegment();
            if (const int e = sealed.flushBufferedToPageCache(); e != 0) {
                markStorageFailed(e);
                throw std::system_error(e, std::generic_category(), "Log seal flush " + dir_);
            }
            sealed.pinFlush();
            pendingSeals_[pendingSealCount_++] = &sealed;
        } else {
            // Legacy (or > kMaxPendingSeals seals outstanding): inline seal fsync.
            activeSegment().sync();
        }
        const int64_t sealedBase = activeSegment().baseOffset();
        int64_t base = logEndOffset_;
        // Install the NEXT segment.  Adopts the background-pre-provisioned staging
        // segment (ZERO syscalls: in-memory rebase; the staging→<base> disk rename
        // is deferred to the bg thread) when ready; otherwise falls back to a
        // synchronous create.  The seal fsync above is unchanged and fully
        // synchronous (durability preserved).
        // E1 review (Kafka: an IOException on roll takes the log dir offline): a failed next-segment
        // create (open / ftruncate / mmap on a dead or full disk) is FAIL-STOP — never a silent retry
        // loop that re-pins the sealed segment and answers every produce with an internal error.
        try {
            installNextSegment(base);
        } catch (const std::system_error& e) {
            markStorageFailed(e.code().value());
            throw;
        } catch (...) {
            markStorageFailed(EIO);
            throw;
        }
        // #5 non-blocking append: preallocate the freshly-installed active segment's
        // blocks (negligible vs the seal fsync above) so its hot-path appends don't
        // stall on block allocation.
        activeSegment().preallocate(rollBytes_);
        // CP-8 FIX (incremental largestTimestamp): capture the running max we
        // tracked for the about-to-be-sealed segment, then reset for the new
        // active segment.  publishSealedSnapshot will use this captured value
        // (via newlySealedMaxTs_) instead of re-scanning the file.
        newlySealedMaxTs_ = activeSegmentMaxTs_;
        newlySealedBase_  = sealedBase;
        activeSegmentMaxTs_ = 0;
        // E1d: an inline seal made the sealed segment durable — R may cross into
        // the new active segment (persisted inline on the legacy path only).
        advanceRecoveryPoint_();
        maybeCheckpointInline_();
        // Publish snapshot: a new sealed segment just appeared.
        publishSealedSnapshot();
        // Clear the seal-time hint after the snapshot consumed it.
        newlySealedMaxTs_ = 0;
        newlySealedBase_  = -1;
    }
}

void Log::publishSealedSnapshot() {
    // Build a fresh immutable snapshot of all sealed segments (all except the
    // last/active).  Called on the single-writer append thread only.
    // The active segment is excluded: its sizeBytes is mutated on appends.
    //
    // CP-8 FIX (incremental largestTimestamp): we NO LONGER re-scan the file for
    // a newly-sealed segment on the hot append path.  Instead, maybeRoll() maintains
    // activeSegmentMaxTs_ (a running max of maxTimestamp() across every batch appended
    // to the active segment) and stores it in newlySealedMaxTs_ just before calling us.
    // For segments already present in the previous snapshot, we reuse the
    // cached largestTimestamp value (sealed segments are immutable).
    // For the newly-sealed segment (not yet in prevSnap), we use newlySealedMaxTs_.
    // Zero extra I/O on the append path: NO file open, NO read, NO heap allocation.
    //
    // CP-5.1 FIX (retention-after-restart): recover() populates recoveryMaxTimestamps_
    // with the REAL per-segment largestTimestamp obtained via a one-time batch scan.
    // When a segment is not found in prevSnap AND newlySealedMaxTs_ == 0 (the recover()
    // publish path), we look up the recovery map before falling back to 0.
    // The map is drained (cleared) here after its first use so it never persists past
    // recovery.  This ensures time-retention gets correct timestamps after a restart.
    auto prevSnap = std::atomic_load(&sealedSnapshot_);

    auto snap = std::make_shared<SealedSegmentSnapshot>();
    if (segments_.size() > 1) {
        snap->reserve(segments_.size() - 1);
        // D11: both segments_ and prevSnap are ascending by baseOffset — one merge
        // walk (O(n) per roll; was a scan of prevSnap per segment, O(n^2) on the
        // append path once a partition holds thousands of segments).
        std::size_t prevIdx = 0;
        auto it  = segments_.begin();
        auto end = std::prev(segments_.end()); // stop before active
        for (; it != end; ++it) {
            int64_t base  = it->first;
            int64_t bytes = it->second->sizeBytes();
            // Exclusive end = next segment's base (the active segment's base for the
            // last sealed entry).  std::next(it) is always valid here (it != end,
            // and end == the active segment).
            int64_t endOff = std::next(it)->first;
            const uint64_t lineage = it->second->tsLineage();
            // Try to reuse largestTimestamp from the previous snapshot for this
            // segment (sealed segments are immutable after they are created).
            // The cache key is (base, lineage): a segment truncateTo() rebuilt
            // and re-appended is a NEW incarnation at an old base — its stale
            // pre-truncate timestamp must never be reused (log-snapshot review
            // MAJOR-1: time retention would delete fresh data early).
            int64_t largestTs = 0;
            bool    found     = false;   // MEDIUM-2: 0 is a real timestamp, not "not found"
            if (prevSnap) {
                const SealedSegmentSnapshot& prev = *prevSnap;
                while (prevIdx < prev.size() && prev[prevIdx].baseOffset < base) ++prevIdx;
                if (prevIdx < prev.size() && prev[prevIdx].baseOffset == base &&
                    prev[prevIdx].lineage == lineage) {
                    largestTs = prev[prevIdx].largestTimestamp;
                    found     = true;
                }
            }
            // If not found in the previous snapshot, this is either:
            //   (a) The NEWLY SEALED segment from maybeRoll(): use newlySealedMaxTs_
            //       (CP-8 fix — zero append-path I/O at seal time).
            //   (b) A segment recovered from disk by recover(): use recoveryMaxTimestamps_
            //       (CP-5.1 fix — real timestamps to prevent retention-after-restart data loss).
            if (!found) {
                if (base == newlySealedBase_) {
                    largestTs = newlySealedMaxTs_;
                } else {
                    // CP-5.1: check the recovery map (populated by recover() batch scan).
                    auto rmIt = recoveryMaxTimestamps_.find(base);
                    if (rmIt != recoveryMaxTimestamps_.end()) {
                        largestTs = rmIt->second;
                    }
                }
            }
            // CRITICAL-1: never below what the segment(s) this one replaced or
            // absorbed published (a compaction rewrite / coalesced merge).
            if (it->second->tsFloor() > largestTs) largestTs = it->second->tsFloor();
            it->second->setPublishedTs(largestTs);
            snap->push_back({base, bytes, largestTs, endOff, compactionAllowedThrough(endOff), lineage});
        }
    }
    // CP-5.1: clear the recovery map after first use — it is only needed for the
    // one-time recover() publishSealedSnapshot() call; subsequent calls (from
    // maybeRoll()/drainCleanerSwaps()) must not inherit stale recovery values.
    recoveryMaxTimestamps_.clear();
    std::atomic_store(&sealedSnapshot_, std::shared_ptr<const SealedSegmentSnapshot>(std::move(snap)));
}
// ---------------------------------------------------------------------------
// Background segment pre-provisioning.
// ---------------------------------------------------------------------------
int Log::preprovisionPending() {
    // BACKGROUND thread only.  Keep exactly ONE staging segment ready at all
    // times so the append thread never pays a create at roll.  NEVER touches
    // segments_ (single-writer invariant): it only builds a detached LogSegment
    // (file I/O) and publishes it via the atomic handoff slot.
    //
    // preprovInFlight_ guards against building two segments and against a race
    // with the append-thread synchronous fallback (installNextSegment):
    //   - We CAS false→true to claim the build.  If it was already true, either a
    //     prepared segment is waiting (append thread hasn't consumed it) or a
    //     build is in progress — either way, nothing to do.
    bool expected = false;
    if (!preprovInFlight_.compare_exchange_strong(expected, true,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        return 0; // already prepared or building
    }
    // If a prepared segment somehow already exists (shouldn't, given the flag),
    // release the claim and bail — never leak a second one.
    if (preparedSegment_.load(std::memory_order_acquire) != nullptr) {
        preprovInFlight_.store(false, std::memory_order_release);
        return 0;
    }
    // Build the staging segment (the expensive open + ftruncate + mmap + prefault
    // all happen HERE, on this background thread).
    const int64_t seq = static_cast<int64_t>(stagingSeq_++);
    std::unique_ptr<LogSegment> seg;
    try {
        seg = LogSegment::prepareStaging(stagingLogPathFor(seq), stagingIndexPathFor(seq), seq,
                                         kDefaultIndexIntervalBytes, indexMaxBytes_);
    } catch (...) {
        // On any failure, release the claim so the append thread falls back to a
        // synchronous create and we retry next cycle.
        preprovInFlight_.store(false, std::memory_order_release);
        return 0;
    }
    // Publish: release-store hands the fully-built segment (files + mmap) to the
    // append thread's acquire-load in installNextSegment().
    preparedSegment_.store(seg.release(), std::memory_order_release);
    return 1;
}

void Log::installNextSegment(int64_t base) {
    // APPEND thread only.  Adopt the pre-provisioned staging segment if ready;
    // else synchronously create one (correctness fallback — slower, only when the
    // bg thread hasn't produced one yet, e.g. right after startup or two rolls in
    // quick succession).
    LogSegment* prepared = preparedSegment_.exchange(nullptr, std::memory_order_acq_rel);
    if (prepared) {
        // ADOPT — ZERO syscalls on the hot path: pure in-memory rebase.  The
        // staging .log/.index keep their temp names on disk for now; we enqueue
        // the disk rename (staging → <base>.{log,index}) for the background
        // thread to perform off the hot path.
        std::unique_ptr<LogSegment> seg(prepared);
        const int64_t stagingSeq = seg->stagingSeq();
        seg->adoptAsActive(base);            // in-memory only
        install_(base, std::move(seg));
        // Enqueue the deferred rename (single-slot-safe SPSC; rolls are rare so
        // the small ring never fills).  If it somehow can't be enqueued, do the
        // rename synchronously as a fallback (still correct, just slower).
        if (!enqueueStagingRename(base, stagingSeq)) {
            renameStagingToBase(stagingSeq, base);
            auto it = segments_.find(base);
            if (it != segments_.end())
                it->second->setLogPath(logPathForBase(base));
        }
        // Release the in-flight claim so the bg thread builds the NEXT staging
        // segment on its following cycle.
        preprovInFlight_.store(false, std::memory_order_release);
    } else {
        // No prepared segment yet (startup, or two rolls closer together than one
        // bg cycle).  Correctness fallback: synchronous create.  A build in
        // progress will publish a segment that the NEXT roll consumes; we do NOT
        // touch preprovInFlight_ here.  This IS a brand-new ACTIVE segment (F3):
        // eagerReadMap=true, matching prepareStaging's own openReadMap() call.
        //
        // F3 fix-round-1: construct into a LOCAL first, and only install into
        // segments_ once construction has fully succeeded.  LogSegment's ctor
        // can now throw (e.g. open() failing — see item 2), and segments_[base]
        // = make_unique<...>(...) would otherwise risk std::map::operator[]
        // inserting a null entry for `base` before the throwing RHS is fully
        // evaluated.  With this ordering, a failed roll leaves segments_
        // completely untouched — the OLD (just-sealed) segment stays the
        // active segment (still a valid, if over-size, entry), the exception
        // propagates to the append() caller, and the next append() retries
        // the roll.  Log is NEVER left without an active segment.
        auto seg = std::make_unique<LogSegment>(
            dir_, base, kDefaultIndexIntervalBytes, indexMaxBytes_,
            /*prefaultIndex*/false, /*eagerReadMap*/true);
        install_(base, std::move(seg));
    }
    // MEDIUM-2: a new directory entry (the bg thread created the adopted
    // staging file before publishing it; the fallback created it just now).
    // A crash before its rename leaves the staging name, which construction
    // reconciles — so the entry is durable once a directory fsync issued
    // after this point completes.
    segments_.rbegin()->second->setDirEntryGen(++dirMutGen_);
}

// --- Staging path helpers (background-pre-provisioning naming convention) ---
// NOTE: staging files use NON-".log"/".index" extensions (.stlog/.stidx) so they
// are invisible to any directory scan that keys off ".log"/".index" (segment
// discovery, retention size counts, tests).  Only the adopt-time rename gives the
// file its final <base>.log/.index name.  The already-open fd/mmap are extension-
// agnostic, so the zero-syscall in-memory adopt is unaffected.
std::string Log::stagingLogPathFor(int64_t seq) const {
    return dir_ + "/" + kStagingPrefix + std::to_string(seq) + ".stlog";
}
std::string Log::stagingIndexPathFor(int64_t seq) const {
    return dir_ + "/" + kStagingPrefix + std::to_string(seq) + ".stidx";
}
std::string Log::logPathForBase(int64_t base) const {
    // Mirror LogSegment::fname formatting: 20-digit zero-padded base.
    std::string s = std::to_string(base);
    if (s.size() < 20) s = std::string(20 - s.size(), '0') + s;
    return dir_ + "/" + s + ".log";
}
std::string Log::indexPathForBase(int64_t base) const {
    std::string s = std::to_string(base);
    if (s.size() < 20) s = std::string(20 - s.size(), '0') + s;
    return dir_ + "/" + s + ".index";
}

void Log::renameStagingToBase(int64_t seq, int64_t base) noexcept {
    // Rename the staging .log/.index files to their final <base>.{log,index}
    // names.  rename() preserves the append thread's open fd_ and index mmap
    // (they keep pointing at the same inode).  Best-effort: if a rename fails the
    // in-memory segment still works (fd/mmap are name-agnostic) and recovery will
    // reconcile on the next restart.
    std::error_code ec;
    std::filesystem::rename(stagingLogPathFor(seq),   logPathForBase(base),   ec);
    std::filesystem::rename(stagingIndexPathFor(seq), indexPathForBase(base), ec);
}

bool Log::enqueueStagingRename(int64_t base, int64_t seq) noexcept {
    // SPSC producer (append thread) → background thread.  Rolls are rare so the
    // small ring never fills in practice; a full ring returns false and the
    // caller renames synchronously (still correct).
    const std::size_t tail = renameReqTail_.load(std::memory_order_relaxed);
    const std::size_t head = renameReqHead_.load(std::memory_order_acquire);
    if (tail - head >= kRenameRingCapacity) return false;
    renameReqRing_[tail & kRenameRingMask] = {base, seq};
    renameReqTail_.store(tail + 1, std::memory_order_release);
    return true;
}

int Log::finalizeStagingRenames() {
    // SPSC consumer (BACKGROUND thread): perform the deferred staging→base disk
    // renames, then publish each completed base to the append thread so it can
    // update logPath_.  NEVER touches segments_.
    const std::size_t head = renameReqHead_.load(std::memory_order_relaxed);
    const std::size_t tail = renameReqTail_.load(std::memory_order_acquire);
    int count = 0;
    for (std::size_t i = head; i != tail; ++i) {
        const auto rr = renameReqRing_[i & kRenameRingMask];
        renameStagingToBase(rr.seq, rr.base);
        // Publish the completed base to the append-thread done-ring (best effort:
        // if the done-ring is full the append thread will still work — the fd/mmap
        // are name-agnostic; logPath_ just stays at the staging name until a later
        // reconcile, and recovery rebuilds correctly regardless).
        const std::size_t dt = renameDoneTail_.load(std::memory_order_relaxed);
        const std::size_t dh = renameDoneHead_.load(std::memory_order_acquire);
        if (dt - dh < kRenameRingCapacity) {
            renameDoneRing_[dt & kRenameRingMask] = rr.base;
            renameDoneTail_.store(dt + 1, std::memory_order_release);
        }
        ++count;
    }
    if (count > 0) renameReqHead_.store(tail, std::memory_order_release);
    return count;
}

int Log::drainStagingRenamesDone() {
    // SPSC consumer (APPEND thread): for each completed rename, update the
    // in-memory logPath_ to the final <base>.log so recover()/reload use the
    // correct on-disk name.  The open fd_/mmap are unaffected.
    const std::size_t head = renameDoneHead_.load(std::memory_order_relaxed);
    const std::size_t tail = renameDoneTail_.load(std::memory_order_acquire);
    int count = 0;
    for (std::size_t i = head; i != tail; ++i) {
        const int64_t base = renameDoneRing_[i & kRenameRingMask];
        auto it = segments_.find(base);
        if (it != segments_.end()) it->second->setLogPath(logPathForBase(base));
        ++count;
    }
    if (count > 0) renameDoneHead_.store(tail, std::memory_order_release);
    return count;
}

void Log::recoverStagingFiles() {
    // Called from the constructor (append thread, no concurrency yet).  Reconcile
    // staging files left by a crash:
    //   - staging.log with data → the segment was adopted in-memory but the bg
    //     rename didn't complete before the crash.  Recover the true base from the
    //     first batch header and rename staging.log → <base>.log (drop the stale
    //     staging.index; recover() rebuilds .index from .log).  NO DATA LOSS.
    //   - empty staging.log (never written) → the segment was pre-provisioned but
    //     never adopted; safe to delete.
    //   - orphan staging.index with no matching staging.log → delete.
    std::error_code ec;
    std::vector<std::filesystem::path> stagingLogs, stagingIdx;
    for (auto& e : std::filesystem::directory_iterator(dir_, ec)) {
        const std::string fn = e.path().filename().string();
        if (fn.rfind(kStagingPrefix, 0) != 0) continue;
        if (e.path().extension() == ".stlog")      stagingLogs.push_back(e.path());
        else if (e.path().extension() == ".stidx") stagingIdx.push_back(e.path());
    }
    for (const auto& lp : stagingLogs) {
        std::ifstream f(lp, std::ios::binary);
        int64_t fileSize = 0;
        if (f.is_open()) { f.seekg(0, std::ios::end); fileSize = f.tellg(); f.seekg(0, std::ios::beg); }
        if (fileSize >= static_cast<int64_t>(rune::protocol::batch::kRecordBatchOverhead)) {
            // Read the first batch header to recover the true base offset.
            std::vector<uint8_t> hdr(static_cast<std::size_t>(
                std::min<int64_t>(fileSize, rune::protocol::batch::kRecordBatchOverhead + 64)));
            f.read(reinterpret_cast<char*>(hdr.data()), static_cast<std::streamsize>(hdr.size()));
            f.close();
            rune::protocol::RecordBatchReader r(hdr.data(), hdr.size());
            int64_t base = r.baseOffset();
            std::filesystem::rename(lp, logPathForBase(base), ec);
            // Drop any co-named staging.index (recover() rebuilds the index).
        } else {
            f.close();
            std::filesystem::remove(lp, ec); // empty / torn — never adopted
        }
    }
    // Remove any leftover staging .index files (rebuilt by recover() when needed).
    for (const auto& ip : stagingIdx) std::filesystem::remove(ip, ec);
}

int64_t Log::append(const uint8_t* data, std::size_t len, int64_t lastOffset) {
    // Zero-alloc hot path: LogSegment::append(pointer) writes directly from the
    // caller's buffer — no std::vector construction on the produce path.
    // CP-8: call maybeRoll() FIRST (which may seal the old segment and reset
    // activeSegmentMaxTs_), THEN update activeSegmentMaxTs_ for the NEW batch
    // being written to the (possibly freshly-started) active segment.
    // This ensures the incremental counter tracks batches that land on the
    // current active segment, NOT the batch that caused the roll.
    throwIfFailed_();
    maybeRoll();
    logEndOffset_ = activeSegment().append(data, len, lastOffset);
    advanceRecoveryPoint_();   // E1d: the durable append synced the active segment
    maybeCheckpointInline_();
    // Update incremental largestTimestamp AFTER the write (and after any roll)
    // so this batch's maxTimestamp is attributed to the segment it was written to.
    if (len >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
        // MAJOR-1 (review fix round 1): EVERY batch of a multi-batch append.
        const int64_t batchMaxTs = maxTimestampOfBatches(data, len);
        if (batchMaxTs > activeSegmentMaxTs_) activeSegmentMaxTs_ = batchMaxTs;
    }
    return logEndOffset_;
}
int64_t Log::append(const std::vector<uint8_t>& batch, int64_t lastOffset) {
    // Vector overload: kept for API compatibility; delegates to the pointer overload.
    return append(batch.data(), batch.size(), lastOffset);
}
int64_t Log::appendNoSync(const uint8_t* data, std::size_t len, int64_t lastOffset) {
    // No-sync group-commit path: write to page cache only; no fsync.
    // maybeRoll() is still needed to start a fresh segment when the active one
    // is full; the new segment starts empty (no prior un-flushed data to lose).
    // The caller (gateway doWork) calls sync() once after the ingress batch.
    // CP-8: same post-roll update pattern as append() above.
    throwIfFailed_();
    maybeRoll();
    logEndOffset_ = activeSegment().appendNoSync(data, len, lastOffset);
    if (len >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
        // MAJOR-1 (review fix round 1): EVERY batch of a multi-batch append.
        const int64_t batchMaxTs = maxTimestampOfBatches(data, len);
        if (batchMaxTs > activeSegmentMaxTs_) activeSegmentMaxTs_ = batchMaxTs;
    }
    return logEndOffset_;
}
int64_t Log::appendNoSyncRestamp(const uint8_t* batch, std::size_t len,
                                 int32_t lastOffsetDelta, int64_t& outBaseOffset) {
    RUNE_FR_SPAN_STORAGE(rune_fr::Site::StorageWrite, 0);
    // Assign the base offset atomically from the log's current end offset, then
    // write zero-copy (pwritev restamp): the assigned base offset lands in the
    // page cache without any mutable copy of `batch`.  maybeRoll() runs first;
    // the base offset is read AFTER the roll so it matches the segment the batch
    // actually lands in.
    throwIfFailed_();
    maybeRoll();
    const int64_t base       = logEndOffset_;
    const int64_t lastOffset = base + static_cast<int64_t>(lastOffsetDelta);
    outBaseOffset = base;
    logEndOffset_ = activeSegment().appendNoSyncRestamp(batch, len, base, lastOffset,
                                                          leaderEpochStamp_);
    if (len >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
        // MAJOR-1 (review fix round 1): EVERY batch of a multi-batch append.
        const int64_t batchMaxTs = maxTimestampOfBatches(batch, len);
        if (batchMaxTs > activeSegmentMaxTs_) activeSegmentMaxTs_ = batchMaxTs;
    }
    return logEndOffset_;
}
std::vector<uint8_t> Log::read(int64_t startOffset, int32_t maxBytes) {
    auto it = segments_.upper_bound(startOffset);
    if (it == segments_.begin()) return {};
    --it;
    return it->second->read(startOffset, maxBytes);
}
LogReadView Log::readView(int64_t startOffset, int32_t maxBytes) noexcept {
    RUNE_FR_SPAN_READ(rune_fr::Site::ReadLogRead, 0);
    ++readViewCalls_;
    auto it = segments_.upper_bound(startOffset);
    if (it == segments_.begin()) return {};
    --it;
    return it->second->readView(startOffset, maxBytes);
}
int64_t Log::logStartOffset() const noexcept {
    // The oldest segment's base offset (segments_ is an ordered map keyed by base).
    // Empty map (should not happen post-construction) → 0.
    if (segments_.empty()) return 0;
    return segments_.begin()->first;
}
std::vector<Log::SyncedExtent> Log::syncedExtentsForTest() const {
    std::vector<SyncedExtent> out;
    out.reserve(segments_.size());
    for (const auto& [base, seg] : segments_)
        out.push_back(SyncedExtent{seg->logPath(), seg->syncedBytesForTest()});
    return out;
}

int64_t Log::recover() {
    // Startup: nothing has been posted to the cleaner yet (no drain ran), so
    // every on-disk name is canonical.
    // E1d: the recovery point recovery trusts up to.
    std::error_code ec;
    std::filesystem::remove(rpTmpPath_, ec);   // a crash mid-creation: never renamed
    const RpFileRead rf = readRecoveryPointFile(dir_);
    int64_t rp = rf.r;
    bool corrupt = false;
    if (rf.state == RpFileState::Corrupt) {
        // F5 (Kafka KafkaStorageException parity): both slots unreadable —
        // nothing can be trusted or deleted.  Fail-stop: CRC-recover only (no
        // chain truncation, no deletion), the log is permanently failed and
        // its owner halts the broker.
        corrupt = true;
        if (storage_.failedErrno == 0) storage_.failedErrno = EIO;
        spdlog::critical("Log::recover {}: {} is corrupt (no valid slot) — log FAILED (fail-stop); "
                         "operator action required", dir_, kRecoveryPointFile);
    } else if (rf.state == RpFileState::Missing) {
        // Upgrade / first boot: the last sealed segment's end.
        rp = segments_.rbegin()->first;
        if (hadSegmentsAtOpen_)
            spdlog::warn("Log::recover {}: no {} — trusting every sealed segment (recovery point = "
                         "last sealed end {}); upgrade / first boot", dir_, kRecoveryPointFile, rp);
    } else {
        rpSeq_ = rf.seq;
        rpActiveSlot_ = rf.slot;
    }
    // MEDIUM-2: the directory fd, opened ONCE (recovery's directory fsync and
    // the io backend's batched ones use it).
    if (dirFd_ < 0) {
        dirFd_ = ::open(dir_.c_str(), O_RDONLY | O_DIRECTORY);
        if (dirFd_ < 0 && storage_.failedErrno == 0) storage_.failedErrno = errno != 0 ? errno : EIO;
    }
    validateFromRecoveryPoint_(corrupt ? INT64_MAX : rp, /*deleteOnGap=*/!corrupt);
    const int64_t next = activeSegment().nextOffset();
    logEndOffset_ = next;
    recovered_ = true;
    if (rf.state == RpFileState::Valid && rp > next)
        spdlog::error("Log::recover {}: the persisted recovery point {} is ABOVE the recovered log end "
                      "{} — durable data vanished from disk", dir_, rp, next);
    // Everything kept is durable now (trusted below rp, fsynced above it):
    // R = LEO, persisted before anything is appended.  No snapshot publish here:
    // recover() publishes once below, AFTER the real per-segment timestamps are
    // known (CP-5.1: a 0 timestamp reads "ancient").
    recoveryPoint_ = next;
    rpDue_ = false;
    // MEDIUM-2: every entry present now (recoverStagingFiles' renames, segment
    // files of earlier incarnations) becomes durable before R = LEO trusts it:
    // the directory fd is opened ONCE (kept for the io backend's batched
    // directory fsyncs) and fsynced here, cold.
    if (!corrupt && storage_.failedErrno == 0) (void)syncDirectoryInline_();
    dirSyncedGen_ = storage_.failedErrno == 0 ? dirMutGen_ : dirSyncedGen_;
    rpDirBlocked_ = false;
    if (!corrupt && storage_.failedErrno == 0) {
        if (rpFd_ < 0 && rf.state == RpFileState::Valid) {
            rpFd_ = ::open(rpPath_.c_str(), O_RDWR);
            if (rpFd_ < 0 && storage_.failedErrno == 0) storage_.failedErrno = errno != 0 ? errno : EIO;
        }
        if (rf.state == RpFileState::Missing) (void)createRecoveryPointFile_(next);
        else if (storage_.failedErrno == 0)  (void)checkpointRecoveryPoint_(/*publish=*/false);
    }

    // CP-5.1 FIX (retention-after-restart): compute the REAL largestTimestamp for
    // every SEALED segment by scanning its batches for the max maxTimestamp() value.
    //
    // Problem: before this fix, publishSealedSnapshot() was called with
    // newlySealedMaxTs_ == 0 (no prevSnap to reuse from, and no new seal just fired),
    // so every sealed segment got largestTimestamp = 0.  The retention predicate in
    // LogCleanerAgent::runRetention() treated largestTimestamp == 0 as "ancient →
    // deletable", so after ANY restart the first retention pass would delete ALL sealed
    // segments on a Delete/CompactDelete topic with retentionMs >= 0 — regardless of
    // the segments' true age.
    //
    // Fix: iterate over all sealed segments (all except the last/active) and read
    // their on-disk batch headers to extract the real maxTimestamp values.  Recovery
    // is a ONE-TIME, non-hot-path operation so a per-segment file read is acceptable
    // (unlike the CP-8 seal-time scan which had to be incremental on the append path).
    // The computed values are stored in recoveryMaxTimestamps_ and consumed by the
    // immediately-following publishSealedSnapshot() call.
    recoveryMaxTimestamps_.clear();
    if (segments_.size() > 1) {
        auto sealedEnd = std::prev(segments_.end()); // stop before active
        for (auto it = segments_.begin(); it != sealedEnd; ++it) {
            int64_t segMaxTs = 0;
            if (fileMaxBatchTimestamp(it->second->logPath(), segMaxTs))
                recoveryMaxTimestamps_[it->first] = segMaxTs;
        }
    }
    // Log-snapshot review: seed the ACTIVE segment's running max from its
    // pre-restart batches — else its seal records only post-restart batches
    // and a segment holding fresh data reads as old (early time retention).
    rescanActiveSegmentMaxTs_();

    // Publish sealed snapshot so the LogCleanerAgent can see pre-existing
    // sealed segments immediately after recovery (without waiting for the first
    // new append to trigger maybeRoll()).  Without this, the cleaner skips the
    // log on the first doWork() call after a restart (sealedSegmentSnapshot()
    // returns nullptr → candidate selection skips this replica) and the
    // persisted checkpoint is never loaded via ensureCheckpoint().
    // CP-5.1: recoveryMaxTimestamps_ is consumed (and cleared) by this call.
    publishSealedSnapshot();
    return next;
}

void Log::validateFromRecoveryPoint_(int64_t rp, bool deleteOnGap) {
    // Trusted region: segments whose end (= the next segment's base) is at or
    // below rp — CRC-recovered (a torn tail there is still cut), never
    // chain-checked: compaction legitimately leaves offset gaps between them.
    auto it = segments_.begin();
    for (; it != segments_.end(); ++it) {
        auto nx = std::next(it);
        if (nx == segments_.end() || nx->first > rp) break;
        it->second->recover();
        it->second->setDurableNext(it->second->nextOffset());
    }
    // Validated region: from the segment holding rp onward, each segment must
    // be CRC-clean AND end exactly where the next one begins.  At the first
    // mismatch / torn tail everything after is deleted (Kafka) — an async seal
    // that never completed must not leave the log claiming offsets it lacks.
    bool deleted = false;
    for (; it != segments_.end(); ++it) {
        const int64_t next = it->second->recover();
        auto nx = std::next(it);
        if (nx == segments_.end()) break;
        if (next == nx->first || !deleteOnGap) continue;
        spdlog::warn("Log::recover {}: segment {} ends at {} but the next segment starts at {} "
                     "(above recovery point {}) — truncating there, deleting {} later segment(s)",
                     dir_, it->first, next, nx->first, rp,
                     static_cast<long long>(std::distance(nx, segments_.end())));
        while (nx != segments_.end()) {
            const std::string lpath = nx->second->logPath();
            std::error_code ec;
            std::filesystem::remove(lpath, ec);
            std::filesystem::remove(indexPathForLog(lpath), ec);
            nx = erase_(nx);
        }
        deleted = true;
        break;
    }
    if (deleteOnGap) {
        // Make the validated region durable (its torn-tail cuts too) before R
        // is persisted as the LEO.
        auto from = segments_.upper_bound(rp);
        if (from != segments_.begin()) --from;
        for (; from != segments_.end(); ++from) {
            if (from->second->fullyDurable() && std::next(from) != segments_.end()) continue;
            (void)from->second->syncForRecovery();   // error: fail-stop (sticky)
        }
        // The deletions themselves: made durable by recover()'s ONE directory
        // fsync, which follows before R is persisted (LOW-C: never twice).
        (void)deleted;
    }
    // The (possibly new) active segment: preallocate as the ctor did.
    activeSegment().preallocate(rollBytes_);
}

// ---------------------------------------------------------------------------
// E1d: durable recovery point.
// ---------------------------------------------------------------------------
namespace {
inline int platformDataSync(int fd) noexcept {
#if defined(__APPLE__)
    return ::fsync(fd);
#else
    return ::fdatasync(fd);
#endif
}
inline void putLe32(uint8_t* p, uint32_t v) noexcept { for (int i = 0; i < 4; ++i) p[i] = static_cast<uint8_t>(v >> (8 * i)); }
inline void putLe16(uint8_t* p, uint16_t v) noexcept { p[0] = static_cast<uint8_t>(v); p[1] = static_cast<uint8_t>(v >> 8); }
inline void putLe64(uint8_t* p, uint64_t v) noexcept { for (int i = 0; i < 8; ++i) p[i] = static_cast<uint8_t>(v >> (8 * i)); }
inline uint32_t getLe32(const uint8_t* p) noexcept { uint32_t v = 0; for (int i = 3; i >= 0; --i) v = (v << 8) | p[i]; return v; }
inline uint16_t getLe16(const uint8_t* p) noexcept { return static_cast<uint16_t>(p[0] | (p[1] << 8)); }
inline uint64_t getLe64(const uint8_t* p) noexcept { uint64_t v = 0; for (int i = 7; i >= 0; --i) v = (v << 8) | p[i]; return v; }
// Slot layout (32 bytes, LE): [0] magic u32 | [4] version u16 | [6] reserved u16
// | [8] seq u64 | [16] R i64 | [24] crc32c u32 over [0,24) | [28] reserved u32.
constexpr std::size_t kRpCrcOffset = 24;
} // namespace

void Log::encodeRpSlot(uint8_t (&out)[kRpSlotBytes], uint64_t seq, int64_t r) noexcept {
    std::memset(out, 0, sizeof out);
    putLe32(out + 0, kRpMagic);
    putLe16(out + 4, kRpVersion);
    putLe64(out + 8, seq);
    putLe64(out + 16, static_cast<uint64_t>(r));
    putLe32(out + kRpCrcOffset, rune::protocol::crc32c(out, kRpCrcOffset));
}

bool Log::decodeRpSlot(const uint8_t* in, uint64_t& seq, int64_t& r) noexcept {
    if (getLe32(in) != kRpMagic || getLe16(in + 4) != kRpVersion) return false;
    if (getLe32(in + kRpCrcOffset) != rune::protocol::crc32c(in, kRpCrcOffset)) return false;
    seq = getLe64(in + 8);
    r   = static_cast<int64_t>(getLe64(in + 16));
    return r >= 0 && seq > 0;
}

Log::RpFileRead Log::readRecoveryPointFile(const std::string& dir) noexcept {
    RpFileRead out;
    const std::string path = dir + "/" + kRecoveryPointFile;
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno != ENOENT) out.state = RpFileState::Corrupt;   // present but unreadable
        return out;
    }
    out.state = RpFileState::Corrupt;
    for (int slot = 0; slot < 2; ++slot) {
        uint8_t buf[kRpSlotBytes];
        const ssize_t n = ::pread(fd, buf, sizeof buf, static_cast<off_t>(slot * kRpSlotStride));
        if (n != static_cast<ssize_t>(sizeof buf)) continue;
        uint64_t seq = 0; int64_t r = 0;
        if (!decodeRpSlot(buf, seq, r)) continue;
        if (out.state != RpFileState::Valid || seq > out.seq) {
            out.state = RpFileState::Valid; out.seq = seq; out.r = r; out.slot = slot;
        }
    }
    ::close(fd);
    if (out.state != RpFileState::Valid) out.r = kRecoveryPointUnknown;
    return out;
}

int Log::createRecoveryPointFile_(int64_t r) noexcept {
    // Cold, once per log lifetime: build the full pre-allocated file under a
    // tmp name, fsync it, rename it into place and FULL-fsync the directory
    // (F3: the directory entry must be durable — never a datasync).
    std::vector<uint8_t> file(kRpFileBytes, 0);
    uint8_t slot[kRpSlotBytes];
    const uint64_t seq = ++rpSeq_;
    encodeRpSlot(slot, seq, r);
    std::memcpy(file.data(), slot, sizeof slot);   // slot 0 valid, slot 1 empty (invalid)
    int err = 0;
    const int fd = ::open(rpTmpPath_.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) err = errno != 0 ? errno : EIO;
    if (err == 0 && storage_.writeFaultForTest != 0) err = storage_.writeFaultForTest;
    if (err == 0 && ::pwrite(fd, file.data(), file.size(), 0) != static_cast<ssize_t>(file.size()))
        err = errno != 0 ? errno : ENOSPC;
    ++storage_.roleChangeSyncs;
    if (err == 0 && storage_.faultForTest != 0) err = storage_.faultForTest;
    if (err == 0 && ::fsync(fd) != 0) err = errno != 0 ? errno : EIO;
    if (err == 0 && ::rename(rpTmpPath_.c_str(), rpPath_.c_str()) != 0) err = errno != 0 ? errno : EIO;
    if (err == 0) {
        const int dfd = ::open(dir_.c_str(), O_RDONLY | O_DIRECTORY);
        ++storage_.roleChangeSyncs;
        if (dfd < 0 || ::fsync(dfd) != 0) err = errno != 0 ? errno : EIO;
        if (dfd >= 0) ::close(dfd);
    }
    if (err != 0) {
        if (fd >= 0) ::close(fd);
        if (storage_.failedErrno == 0) storage_.failedErrno = err;
        spdlog::critical("Log {}: creating {} FAILED (errno={}) — log failed", dir_, kRecoveryPointFile, err);
        return err;
    }
    rpFd_ = fd;   // the renamed inode: kept open for every later slot write
    rpActiveSlot_ = 0;
    persistedRp_ = r;
    ++rpPersists_;
    return 0;
}

int Log::checkpointRecoveryPoint() noexcept { return checkpointRecoveryPoint_(/*publish=*/true); }

int Log::checkpointRecoveryPoint_(bool publish) noexcept {
    if (!recovered_) return 0;
    if (storage_.failedErrno != 0) return storage_.failedErrno;
    if (rpFd_ < 0) return 0;
    // Target slot: never the one an in-flight async write may be landing in.
    // A Prepared / InFlight checkpoint is superseded: its completion never
    // moves persistedRp_ (this write carries a HIGHER seq).
    int target;
    if (rp_.stage == RpStage::InFlight) {
        target = 1 - rp_.slot;            // the in-flight write keeps its slot
        rp_.abandoned = true;
    } else {
        if (rp_.stage == RpStage::Prepared) rp_ = RpCheckpoint{RpStage::Idle, false, -1, 0, 0};
        target = rpActiveSlot_ < 0 ? 0 : 1 - rpActiveSlot_;
    }
    const int64_t v = recoveryPoint_;
    uint8_t img[kRpSlotBytes];
    const uint64_t seq = ++rpSeq_;
    encodeRpSlot(img, seq, v);
    int err = 0;
    if (storage_.writeFaultForTest != 0) err = storage_.writeFaultForTest;
    else if (::pwrite(rpFd_, img, sizeof img, static_cast<off_t>(target * kRpSlotStride)) != static_cast<ssize_t>(sizeof img))
        err = errno != 0 ? errno : ENOSPC;
    ++storage_.roleChangeSyncs;
    if (err == 0 && storage_.faultForTest != 0) err = storage_.faultForTest;
    if (err == 0 && platformDataSync(rpFd_) != 0) err = errno != 0 ? errno : EIO;
    if (err != 0) {
        if (storage_.failedErrno == 0) storage_.failedErrno = err;
        spdlog::critical("Log {}: persisting the recovery point {} FAILED (errno={}) — log failed",
                         dir_, v, err);
        return err;
    }
    const bool moved = v != persistedRp_;
    persistedRp_ = v;
    rpUnconfirmedMax_ = kRecoveryPointUnknown;   // every possibly-landed write has a LOWER seq
    rpActiveSlot_ = target;
    ++rpPersists_;
    refreshRpDue_();
    if (publish && moved && !segments_.empty()) publishSealedSnapshot();   // cleaner gate
    return 0;
}

void Log::refreshRpDue_() noexcept {
    if (recoveryPoint_ <= persistedRp_) { rpDue_ = false; return; }
    const int64_t b = nextSegmentBaseAfter(persistedRp_);
    rpDue_ = b != -1 && b <= recoveryPoint_;
}

void Log::advanceRecoveryPoint_() noexcept {
    if (segments_.empty()) return;
    auto it = segments_.upper_bound(recoveryPoint_);
    if (it != segments_.begin()) --it;
    int64_t r = recoveryPoint_;
    bool dirBlocked = false;
    while (it != segments_.end()) {
        LogSegment& seg = *it->second;
        auto nx = std::next(it);
        if (seg.dirEntryGen() > dirSyncedGen_) [[unlikely]] {
            // MEDIUM-2: never past the base of a segment whose directory entry
            // is not durable yet.  Blocked = R would otherwise progress here.
            dirBlocked = seg.durableNext() > r ||
                         (nx != segments_.end() && seg.fullyDurable() && nx->first > r);
            break;
        }
        if (seg.durableNext() > r) r = seg.durableNext();
        if (nx == segments_.end() || !seg.fullyDurable()) break;
        if (nx->first > r) r = nx->first;   // a sealed segment fully durable: R reaches the next base
        it = nx;
    }
    rpDirBlocked_ = dirBlocked;
    if (r <= recoveryPoint_) return;
    recoveryPoint_ = r;
    if (!rpDue_) refreshRpDue_();
}

void Log::maybeCheckpointInline_() noexcept {
    // MEDIUM-2, legacy (inline-seal) logs only: the roll already fsyncs
    // inline; the directory fsync that unblocks R is inline too.
    if (rpDirBlocked_ && !deferSealSync_ && recovered_ && dirFd_ >= 0 && storage_.failedErrno == 0) [[unlikely]] {
        if (syncDirectoryInline_() != 0) return;   // fail-stop (sticky)
        advanceRecoveryPoint_();
    }
    if (rpDue_ && !deferSealSync_ && recovered_ && storage_.failedErrno == 0 &&
        rp_.stage == RpStage::Idle)
        (void)checkpointRecoveryPoint();
}

bool Log::noteFlushCompleted(LogSegment* seg, int64_t upTo) noexcept {
    if (seg == nullptr) return false;
    seg->markDurableThrough(upTo);
    advanceRecoveryPoint_();
    return recoveryCheckpointDue() || directorySyncDue();
}

bool Log::prepareRecoveryCheckpoint(RpWrite& out) noexcept {
    if (!recoveryCheckpointDue()) return false;
    const int slot = rpActiveSlot_ < 0 ? 0 : 1 - rpActiveSlot_;
    const uint64_t seq = ++rpSeq_;
    encodeRpSlot(rpImage_, seq, recoveryPoint_);
    rp_ = RpCheckpoint{RpStage::Prepared, false, slot, seq, recoveryPoint_};
    rpDue_ = false;
    out.fd = rpFd_; out.data = rpImage_; out.len = static_cast<uint32_t>(kRpSlotBytes);
    out.offset = static_cast<int64_t>(slot * kRpSlotStride); out.seq = seq;
    return true;
}

void Log::recoveryCheckpointSubmitted() noexcept {
    if (rp_.stage != RpStage::Prepared) return;
    rp_.stage = RpStage::InFlight;
    // MAJOR-1: from now on the slot MAY hold this value (until a confirmed
    // durable write with a higher seq dominates it).
    if (rp_.value > rpUnconfirmedMax_) rpUnconfirmedMax_ = rp_.value;
}

void Log::recoveryCheckpointNotSubmitted() noexcept {
    if (rp_.stage != RpStage::Prepared) return;
    rp_ = RpCheckpoint{RpStage::Idle, false, -1, 0, 0};
    refreshRpDue_();   // still due: retried by the owner
}

bool Log::recoveryCheckpointCompleted(uint64_t seq, int err) noexcept {
    if (rp_.stage != RpStage::InFlight || seq != rp_.seq) return false;   // long gone
    const RpCheckpoint done = rp_;
    rp_ = RpCheckpoint{RpStage::Idle, false, -1, 0, 0};
    if (err != 0) {
        if (storage_.failedErrno == 0) storage_.failedErrno = err;
        spdlog::critical("Log {}: async recovery-point checkpoint {} FAILED (errno={}) — log failed",
                         dir_, done.value, err);
        return false;
    }
    if (!done.abandoned) {
        // DURABLE now (write + fdatasync completed): only now may the cleaner
        // gate and the persisted R move.
        const bool moved = done.value != persistedRp_;
        persistedRp_ = done.value;
        rpUnconfirmedMax_ = kRecoveryPointUnknown;   // the highest-seq write: confirmed
        rpActiveSlot_ = done.slot;
        ++rpPersists_;
        if (moved && !segments_.empty()) publishSealedSnapshot();   // cold: ≈ once per roll
    }
    refreshRpDue_();
    return recoveryCheckpointDue() || directorySyncDue();
}

void Log::resetRecoveryCheckpointForNewOwner() noexcept {
    if (rp_.stage == RpStage::Idle) return;
    // Unknown outcome: the slot (never the active one) may or may not hold it.
    // The next write targets that slot again with a higher seq; persistedRp_
    // (what the cleaner trusts) never moves on it.  MAJOR-1: the value is
    // NOT forgotten — it stays in rpUnconfirmedMax_ (raised at submit) as an
    // upper bound of what may be on disk, so a later truncation below it is
    // persisted inline first.
    if (rp_.stage == RpStage::InFlight && !rp_.abandoned && rp_.value > rpUnconfirmedMax_)
        rpUnconfirmedMax_ = rp_.value;
    rp_ = RpCheckpoint{RpStage::Idle, false, -1, 0, 0};
    refreshRpDue_();
}

std::size_t Log::requeueUndurableSeals(const LogSegment* const* inFlight, std::size_t nInFlight) noexcept {
    std::size_t n = 0;
    if (segments_.size() < 2) return 0;
    const auto activeIt = std::prev(segments_.end());
    for (auto it = segments_.begin(); it != activeIt; ++it) {
        LogSegment* seg = it->second.get();
        if (seg->fullyDurable()) continue;
        bool queued = false;
        for (std::size_t i = 0; i < pendingSealCount_; ++i) queued = queued || pendingSeals_[i] == seg;
        for (std::size_t i = 0; i < nInFlight; ++i) queued = queued || inFlight[i] == seg;   // routed: its completion will come
        if (queued) continue;
        if (pendingSealCount_ >= kMaxPendingSeals) break;   // the rest: next requeue
        seg->pinFlush();
        pendingSeals_[pendingSealCount_++] = seg;
        ++n;
    }
    return n;
}

void Log::persistAtMost_(int64_t bound) noexcept {
    if (recoveryPoint_ > bound) recoveryPoint_ = bound;
    if (!recovered_) return;
    // An in-progress checkpoint carrying a value above the bound must never
    // count (its slot may still land: an inline write with a HIGHER seq to
    // the other slot supersedes it).
    // MAJOR-1: compare against the UPPER BOUND of what may be on disk (the
    // confirmed value and every possibly-landed write), never just the
    // confirmed one.
    const bool staleInProgress = rp_.stage != RpStage::Idle && !rp_.abandoned && rp_.value > bound;
    if (staleInProgress || onDiskRecoveryPointUpperBound() > bound) (void)checkpointRecoveryPoint();
    else refreshRpDue_();
}

bool Log::directorySyncDue() const noexcept {
    return rpDirBlocked_ && !dirSyncInFlight_ && recovered_ && dirFd_ >= 0 &&
           storage_.failedErrno == 0 && dirMutGen_ > dirSyncedGen_;
}

bool Log::prepareDirectorySync(DirSync& out) noexcept {
    if (!directorySyncDue()) return false;
    dirSyncInFlight_ = true;
    dirSyncInFlightGen_ = dirMutGen_;   // every mutation so far has returned: the fsync covers it
    out.fd = dirFd_; out.gen = dirMutGen_;
    return true;
}

void Log::directorySyncNotSubmitted() noexcept { dirSyncInFlight_ = false; }

bool Log::directorySyncCompleted(uint64_t gen, int err) noexcept {
    if (!dirSyncInFlight_ || gen != dirSyncInFlightGen_) return false;   // long gone
    dirSyncInFlight_ = false;
    if (err != 0) {
        if (storage_.failedErrno == 0) storage_.failedErrno = err;
        spdlog::critical("Log {}: async directory fsync FAILED (errno={}) — log failed", dir_, err);
        return false;
    }
    if (gen > dirSyncedGen_) dirSyncedGen_ = gen;
    ++dirSyncsDone_;
    advanceRecoveryPoint_();
    return recoveryCheckpointDue() || directorySyncDue();
}

void Log::resetDirectorySyncForNewOwner() noexcept { dirSyncInFlight_ = false; }

int Log::syncDirectoryInline_() noexcept {
    if (dirFd_ < 0) return 0;
    const uint64_t gen = dirMutGen_;   // counted in dirSyncsDone_ only (LOW-C: not a role-change sync)
    int err = storage_.faultForTest;
    if (err == 0 && ::fsync(dirFd_) != 0) err = errno != 0 ? errno : EIO;
    if (err != 0) {
        if (storage_.failedErrno == 0) storage_.failedErrno = err;
        spdlog::critical("Log {}: directory fsync FAILED (errno={}) — log failed", dir_, err);
        return err;
    }
    if (gen > dirSyncedGen_) dirSyncedGen_ = gen;
    ++dirSyncsDone_;
    return 0;
}

void Log::sync() {
    activeSegment().sync();   // throws + marks failed on error (LogSegment::sync)
    advanceRecoveryPoint_();  // E1d
    maybeCheckpointInline_();
}

void Log::throwIfFailed_() const {
    if (storage_.failedErrno != 0) [[unlikely]]
        throw std::system_error(storage_.failedErrno, std::generic_category(),
                                "Log append refused: storage failed (" + dir_ + ")");
}

void Log::install_(int64_t base, std::unique_ptr<LogSegment> seg) {
    seg->setStorageState(&storage_);
    seg->setInstallGen(++segInstallGen_);   // F15 S4 N1: identity, never reused
    // A fresh content lineage (sealed-snapshot timestamp cache key); a reload of
    // the same sealed content re-sets the inherited one after install_.
    seg->setTsLineage(seg->installGen());
    auto it = segments_.find(base);
    if (it != segments_.end()) {
        // MEDIUM-2: a rebuilt / reloaded object over the SAME file name keeps
        // its entry's generation (callers that also mutated the directory
        // raise it afterwards).
        if (seg->dirEntryGen() == 0 && it->second) seg->setDirEntryGen(it->second->dirEntryGen());
        retire_(std::move(it->second));
        it->second = std::move(seg);
    } else {
        segments_.emplace(base, std::move(seg));
    }
}

Log::SegMap::iterator Log::erase_(SegMap::iterator it) {
    retire_(std::move(it->second));
    return segments_.erase(it);
}

void Log::retire_(std::unique_ptr<LogSegment> seg) {
    if (!seg) return;
    // A pending (un-submitted) seal on a removed segment is dropped from the
    // queue — the data it covered is gone (truncate) or already superseded.
    for (std::size_t i = 0; i < pendingSealCount_; ) {
        if (pendingSeals_[i] == seg.get()) {
            seg->unpinFlush();
            ++droppedPendingSeals_;
            for (std::size_t j = i + 1; j < pendingSealCount_; ++j) pendingSeals_[j - 1] = pendingSeals_[j];
            --pendingSealCount_;
        } else {
            ++i;
        }
    }
    if (seg->flushPins() > 0) retiredSegments_.push_back(std::move(seg));   // M1: fd stays open
    // else: destroyed here (fd closed) — no flush in flight on it.
}

void Log::popPendingSeal() noexcept {
    if (pendingSealCount_ == 0) return;
    for (std::size_t j = 1; j < pendingSealCount_; ++j) pendingSeals_[j - 1] = pendingSeals_[j];
    --pendingSealCount_;
}

void Log::unpinSegmentFlush(LogSegment* seg) noexcept {
    if (!seg || seg->unpinFlush() != 0) return;
    for (std::size_t i = 0; i < retiredSegments_.size(); ++i) {
        if (retiredSegments_[i].get() == seg) {
            retiredSegments_[i] = std::move(retiredSegments_.back());
            retiredSegments_.pop_back();   // closes the fd: no flush in flight any more
            return;
        }
    }
}
int64_t Log::nextSegmentBaseAfter(int64_t offset) const noexcept {
    const auto it = segments_.upper_bound(offset);
    return it == segments_.end() ? -1 : it->first;
}
int64_t Log::segmentNextOffsetAt(int64_t offset) const noexcept {
    auto it = segments_.upper_bound(offset);
    if (it == segments_.begin()) return -1;
    --it;
    return it->second->nextOffset();
}
Log::SegmentBytes Log::segmentBytesAt(int64_t segmentBase) {
    const auto it = segments_.find(segmentBase);
    if (it == segments_.end()) return {};
    SegmentBytes b;
    b.gen = it->second->installGen();
    b.fd  = it->second->logFd();
    const LogReadView v = it->second->mappedBytes();
    b.data = v.data;
    b.size = v.len;
    return b;
}
std::vector<int64_t> Log::segmentBaseOffsets() const {
    std::vector<int64_t> v; for (auto& [b, _] : segments_) v.push_back(b); return v;
}
int64_t Log::totalBytes() const {
    int64_t t = 0; for (auto& [b, seg] : segments_) t += seg->sizeBytes(); return t;
}
bool Log::truncateTo(int64_t truncateOffset) {
    if (truncateOffset >= logEndOffset_) return true; // nothing to do
    // Fix round 5: never touch a file by path while the cleaner still has
    // posted renames to perform — deferred, no wait (the caller retries).
    if (!beginColdPath_()) return false;
    // E1d F6: the lowered recovery point is DURABLE before any file is deleted
    // or resized (a crash in between must never trust bytes about to go).
    persistAtMost_(std::max<int64_t>(0, truncateOffset));
    // Fail-stop: a failed persist leaves every file untouched (the log is
    // permanently failed; its owner halts the broker; restart recovers).
    if (recovered_ && storage_.failedErrno != 0) [[unlikely]] return true;
    // F30 fix round 1: the owner's flush bookkeeping must learn the LOWEST
    // point truncated to, even when new bytes are re-appended before it looks
    // (LEO alone hides that).  Recorded now (an exception below still lowered
    // the content) and again with the exact resulting LEO at each exit.
    noteTruncatedTo_(std::max<int64_t>(0, truncateOffset));
    ++truncations_;

    // Remove segments that start at or beyond truncateOffset.
    auto it = segments_.lower_bound(truncateOffset);
    const bool deletesFiles = it != segments_.end();   // MEDIUM-2: a directory mutation
    while (it != segments_.end()) {
        std::string lpath = it->second->logPath();
        std::string ipath = indexPathForLog(lpath);
        std::filesystem::remove(lpath);
        std::filesystem::remove(ipath);
        it = erase_(it);
    }

    if (segments_.empty()) {
        // Re-create a fresh segment at truncateOffset (or 0 if negative).  This
        // is the ONLY (hence active) segment — F3: eagerReadMap=true.
        // F3 fix-round-1: construct into a LOCAL first (see installNextSegment's
        // comment) — segments_ is already empty here (the loop above erased
        // everything), so this is the only chance to get an active segment at
        // all; a throw must leave segments_ cleanly empty (clean exception,
        // retryable) rather than risk a null entry via operator[].
        int64_t base = std::max<int64_t>(0, truncateOffset);
        auto seg = std::make_unique<LogSegment>(dir_, base, kDefaultIndexIntervalBytes,
                                                 indexMaxBytes_, /*prefaultIndex*/false,
                                                 /*eagerReadMap*/true);
        install_(base, std::move(seg));
        segments_.rbegin()->second->setDirEntryGen(++dirMutGen_);   // MEDIUM-2: deletions + re-create
        logEndOffset_ = base;
        activeSegmentMaxTs_ = 0;   // the fresh active segment holds nothing yet
        // Fix round 4: a full truncation below the persisted log start (unclean
        // election onto a far shorter leader) lowers it FIRST — else the next
        // construction would drop the segments written from `base` on.
        if (base < persistedLogStart_ && !writeLogStartCheckpoint_(base)) {
            markStorageFailed(EIO);   // fail-stop: a stale higher start would drop data at restart
        }
        noteTruncatedTo_(logEndOffset_);
        persistAtMost_(logEndOffset_);   // E1d (batch-rounded LEO may be lower)
        // Log-snapshot review MAJOR-1: segments_ changed (all erased, one
        // re-created) — the cleaner must never see the erased bases.
        publishSealedSnapshot();
        return true;
    }

    // The active segment now straddles truncateOffset.
    // We need to find the byte position of truncateOffset inside it and truncate.
    LogSegment& seg = activeSegment();
    // Recover re-scans the segment file and truncates the torn tail;
    // we first physically truncate the file to the right position by
    // reading batches until we reach truncateOffset.
    std::string lpath = seg.logPath();
    // Read entire segment bytes.
    std::ifstream f(lpath, std::ios::binary);
    f.seekg(0, std::ios::end);
    std::streamsize fileSize = f.tellg();
    f.seekg(0, std::ios::beg);
    std::vector<uint8_t> buf(static_cast<std::size_t>(fileSize));
    if (fileSize > 0) f.read(reinterpret_cast<char*>(buf.data()), fileSize);
    f.close();

    // Walk batches to find the byte position to truncate.
    //
    // cutPos: the physical byte boundary where we cut the file.
    //   Initialized to 0 (safe: truncate everything) rather than buf.size()
    //   (which would silently keep everything if no batch is found — C1 fix).
    // validEnd: byte position after the last batch whose baseOffset < truncateOffset.
    //   Updated as we walk forward past pre-boundary batches.
    //
    // Semantics: truncation rounds DOWN to the batch boundary. A truncateOffset
    // that lands inside a multi-record batch drops that entire batch (same as Kafka).
    std::size_t cutPos  = 0;
    std::size_t validEnd = 0;
    // Log-snapshot review MAJOR-1: the active segment's running max is the
    // KEPT prefix's — never the erased segments' / dropped batches' maximum.
    int64_t keptMaxTs = 0;
    bool found = false;
    std::size_t pos = 0;
    while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= buf.size()) {
        rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
        int32_t total = r.totalLength();
        if (total <= 0 || pos + static_cast<std::size_t>(total) > buf.size()) break;
        // A3: CRC-gate the walk — a corrupt batch terminates the scan so we
        // don't misparse a corrupt totalLength as a valid batch boundary.
        if (!r.ensureValid()) break;

        // A batch must be dropped if ANY of its records is at offset >= truncateOffset.
        // Since records in a batch are contiguous [baseOffset .. baseOffset+lastOffsetDelta],
        // we drop the batch if baseOffset >= truncateOffset (pure overlap from the start),
        // OR if the batch CONTAINS records at >= truncateOffset (i.e. mid-batch cut is
        // impossible, so we round down to the batch's start — drop the whole batch).
        int64_t batchLastOffset = r.baseOffset() + r.lastOffsetDelta();
        if (r.baseOffset() >= truncateOffset || batchLastOffset >= truncateOffset) {
            // This batch contains at least one record at offset >= truncateOffset — cut here.
            cutPos = pos;
            found = true;
            break;
        }
        // This batch is fully before the truncation boundary.
        if (const int64_t ts = toEpochMs(r.maxTimestamp()); ts > keptMaxTs) keptMaxTs = ts;
        validEnd = pos + static_cast<std::size_t>(total);
        pos = validEnd;
    }
    if (!found) {
        // No batch containing any record at offset >= truncateOffset was found.
        // All existing batches are fully below the boundary: keep them all.
        cutPos = validEnd;
    }

    // Truncate the file at cutPos.
    std::filesystem::resize_file(lpath, static_cast<std::uintmax_t>(cutPos));
    // ===== POINT OF NO RETURN =====
    // F3 fix-round-2, item B.2: the .log file is now PHYSICALLY truncated on
    // disk.  Everything from here must either fully succeed or FAIL-STOP.
    // The OLD in-memory `seg` still has its read mmap sized for the PRE-
    // truncate file: if we threw here and silently continued using it, a
    // readView() past the new (shorter) EOF risks SIGBUS, and disk/memory
    // would permanently disagree.  This is a genuine storage failure, not a
    // retryable condition — unlike every other reconstruction path (B.1,
    // round-1's item 2), there is no "leave segments_ untouched and return
    // the exception to the caller" option once the disk write has already
    // happened.  Ruling: log critical (path + errno-bearing message) then
    // std::abort() — Kafka halts a broker on a log-dir I/O failure the same
    // way; the node restarts and recovers cleanly from disk, and replication
    // heals the gap.  Construct the replacement BEFORE erasing the old
    // entry, so a failure here leaves segments_ holding the OLD (stale but
    // structurally valid) segment right up until the abort.
    //
    // F3 round 5, item I4: the guard covers EVERYTHING after the truncate —
    // the index removal, the segment reopen, the install into segments_, the
    // rebuilt segment's recover() (index rebuild: .index.tmp remove/write/
    // rename) AND the logEndOffset_ assignment.  Round 2 left recover() and
    // the LEO assignment outside the guard, so a throw there escaped as an
    // ordinary exception with the .log already truncated and LEO stale.
    int64_t base = seg.baseOffset();
    const int64_t oldDurable = seg.durableNext();   // E1d: the kept prefix stays as durable as it was
    try {
        // Also truncate the index — simplest: just let recover() rebuild it.
        std::string ipath = indexPathForLog(lpath);
        std::filesystem::remove(ipath);
        // Rebuild the active segment from disk.  It IS the active segment
        // (this code only reaches here when truncateOffset falls inside it)
        // — F3: eagerReadMap=true.
        auto newSeg = std::make_unique<LogSegment>(dir_, base, kDefaultIndexIntervalBytes,
                                                    indexMaxBytes_, /*prefaultIndex*/false,
                                                    /*eagerReadMap*/true);
        // E2 crash seam: the surviving prefix keeps what was already synced.
        newSeg->inheritSyncedBytesForTest(
            std::min<int64_t>(seg.syncedBytesForTest(), static_cast<int64_t>(cutPos)));
        // T10: install_ retires the old segment (kept open while a flush is
        // in flight on it — M1) instead of closing its fd under the flush.
        install_(base, std::move(newSeg));
        // Re-run recover to rebuild the index and set nextOffset correctly.
        // Use the segment's actual nextOffset_ (derived from the physically
        // truncated file) rather than hard-coding truncateOffset, so that LEO
        // and the segment's nextOffset_ can never diverge.
        activeSegment().recover();
        activeSegment().setDurableNext(std::min(oldDurable, activeSegment().nextOffset()));
        // MEDIUM-2: R stays below the deleted files' range until a directory
        // fsync covered the deletions (else they could reappear trusted).
        if (deletesFiles) activeSegment().setDirEntryGen(++dirMutGen_);
        logEndOffset_ = activeSegment().nextOffset();
    } catch (const std::exception& ex) {
        fatalHalt("Log::truncateTo: post-truncate storage failure rebuilding segment "
                  "dir=" + dir_ + " base=" + std::to_string(base) + " path=" + lpath +
                  ": " + ex.what() + ".  The .log file is ALREADY truncated on disk "
                  "(cutPos=" + std::to_string(cutPos) + "); disk and memory would now "
                  "permanently disagree.  Halting (Kafka-style log-dir-failure halt) "
                  "rather than risk SIGBUS or silent corruption — restart recovers "
                  "cleanly from disk.");
    } catch (...) {
        fatalHalt("Log::truncateTo: post-truncate storage failure rebuilding segment "
                  "dir=" + dir_ + " base=" + std::to_string(base) + " path=" + lpath +
                  ": unknown exception.  The .log file is ALREADY truncated on disk "
                  "(cutPos=" + std::to_string(cutPos) + ").  Halting.");
    }
    activeSegmentMaxTs_ = keptMaxTs;
    noteTruncatedTo_(logEndOffset_);   // rounded down to the batch boundary
    persistAtMost_(logEndOffset_);   // E1d: a mid-batch cut rounds the LEO below truncateOffset
    // Log-snapshot review MAJOR-1: segments_ changed (later segments erased, a
    // formerly sealed segment rebuilt as the ACTIVE one) — republish so the
    // cleaner's activeBase / sealed set match.  The only earlier publish (inside
    // the first persistAtMost_) ran BEFORE the erase.
    publishSealedSnapshot();
    return true;
}

bool Log::reloadSegments() {
    if (!beginColdPath_()) return false;   // fix round 5: posted renames first (deferred, no wait)
    // Re-discover segment files on disk (same logic as the constructor).
    std::vector<int64_t> bases;
    for (auto& e : std::filesystem::directory_iterator(dir_))
        if (e.path().extension() == ".log")
            bases.push_back(std::stoll(e.path().stem().string()));
    std::sort(bases.begin(), bases.end());
    if (bases.empty()) bases.push_back(0);

    // F3 fix-round-2, item B.1: build the COMPLETE new segment generation
    // (construct AND recover every segment) into a LOCAL map first — STRONG
    // exception guarantee.  segments_ (the OLD generation, still fully valid
    // and open) is NOT touched until every step below has fully succeeded.
    // Before this fix, segments_.clear() ran FIRST: a throwing ctor or a
    // throwing recover() partway through rediscovery left segments_ either
    // completely empty or missing its active (highest-base) entry —
    // activeSegment() on that map was silent UB (now also hard-asserted,
    // item B.3).  Now: on ANY failure here, segments_ is left EXACTLY as it
    // was before this call — the old segments are still open and usable, the
    // exception propagates cleanly, and the caller may retry.
    std::map<int64_t, std::unique_ptr<LogSegment>> newSegments;
    for (int64_t b : bases) {
        // F3: same eager-only-for-the-active-segment rule as the constructor.
        newSegments[b] = std::make_unique<LogSegment>(dir_, b, kDefaultIndexIntervalBytes,
                                                       indexMaxBytes_, /*prefaultIndex*/false,
                                                       /*eagerReadMap*/ b == bases.back());
        newSegments[b]->setStorageState(&storage_);   // T10: recover() sees the failure state
        // F15 S4 N1 (merged with F3's install-by-swap): every installed segment
        // carries a fresh Log-wide generation — install_ is bypassed here, so
        // stamp it now (a later throw only skips numbers; never reused).
        newSegments[b]->setInstallGen(++segInstallGen_);
        // CRITICAL-1: every reloaded object is a NEW incarnation (the file may
        // be a compaction rewrite / coalesced merge) — fresh lineage; its
        // timestamp starts from what the object it replaces published.
        newSegments[b]->setTsLineage(newSegments[b]->installGen());
        auto old = segments_.find(b);
        if (old != segments_.end() && old->second) {
            newSegments[b]->raiseTsFloor(old->second->publishedTs());
            newSegments[b]->raiseTsFloor(old->second->tsFloor());
        }
    }
    // A base whose file vanished (merged into / emptied by the cleaner) folds
    // its timestamp into the segment that now covers its offsets.
    for (auto& [b, seg] : segments_) {
        if (!seg || newSegments.count(b) != 0) continue;
        auto covering = newSegments.upper_bound(b);
        if (covering == newSegments.begin()) continue;
        --covering;
        covering->second->raiseTsFloor(seg->publishedTs());
        covering->second->raiseTsFloor(seg->tsFloor());
    }
    // Recover BEFORE installing — extends the same strong guarantee to the
    // whole reconstruction, not just segment construction: a throwing
    // recover() also leaves segments_ (the old generation) untouched.
    int64_t next = 0;
    for (auto& [base, seg] : newSegments) {
        next = seg->recover();
        // E1d: a rewritten (cleaner: fsynced before rename) or untouched segment
        // is as durable as the object it replaces; a brand-new base is not.
        auto old = segments_.find(base);
        seg->setDirEntryGen(old != segments_.end() && old->second ? old->second->dirEntryGen() : ++dirMutGen_);
        if (old != segments_.end() && old->second)
            seg->setDurableNext(old->second->fullyDurable() ? seg->nextOffset()
                                                            : std::min(old->second->durableNext(), seg->nextOffset()));
    }

    // Everything succeeded — now replace the generation with NO step that can
    // throw once the old generation is touched (F3 round 7):
    //   1. Allocate first: retiring a pinned old segment pushes onto
    //      retiredSegments_ (T10 M1: its fd stays open until the in-flight
    //      flush completes) — reserve that capacity up front.  A bad_alloc
    //      here propagates with segments_ (the old generation) UNTOUCHED.
    //   2. Retire every old segment (push_back into reserved capacity +
    //      fixed-array pending-seal bookkeeping: no allocation, no throw).
    //   3. Install the new generation with std::map::swap (noexcept).  Every
    //      new segment already carries the storage-failure state (set before
    //      its recover() above) and its install generation (F15 N1) — all
    //      install_ would add.
    if (reloadAllocFaultForTest_) [[unlikely]] throw std::bad_alloc();
    retiredSegments_.reserve(retiredSegments_.size() + segments_.size());
    for (auto& [b, seg] : segments_) retire_(std::move(seg));
    segments_.swap(newSegments);   // old (now all-null) entries die with newSegments
    logEndOffset_ = next;
    if (recoveryPoint_ > logEndOffset_) recoveryPoint_ = logEndOffset_;   // E1d (never above the LEO)
    // Log-snapshot review audit: segments_ was rebuilt (a cleaner-emptied
    // segment may be gone) — re-derive the active max and republish (cold).
    rescanActiveSegmentMaxTs_();
    publishSealedSnapshot();
    return true;
}

void Log::rescanActiveSegmentMaxTs_() {
    int64_t ts = 0;
    activeSegmentMaxTs_ = fileMaxBatchTimestamp(activeSegment().logPath(), ts) ? ts : 0;
}

void Log::reloadSegment(int64_t baseOffset) {
    reloadSegmentNoPublish_(baseOffset);
    // Log-snapshot review audit: a direct (synchronous-cleaner) reload mutates
    // segments_ too — the cleaner thread must see the new size / removal.
    publishSealedSnapshot();
}

void Log::reloadSegmentNoPublish_(int64_t baseOffset) {
    // CP-2: in-place single-segment reload after a cleaner rewrite.
    //
    // The cleaner atomically renamed <base>.log.new -> <base>.log (or, when the
    // segment was fully emptied, deleted <base>.log entirely).  We must refresh
    // ONLY that segment's in-memory LogSegment so its open fd points at the new
    // inode — without re-recover()-ing every other segment (the O(n) cost that
    // makes a per-segment compaction loop O(n^2)).
    //
    // logEndOffset_ is NOT touched: sealed-segment compaction never changes the
    // LEO (the active segment, which owns the LEO, is never compacted).
    auto it = segments_.find(baseOffset);
    if (it == segments_.end()) return; // unknown segment — nothing to do
    // MAJOR-2: the ACTIVE segment is never reloaded or dropped (it owns the LEO;
    // a request naming it is stale — the cleaner acted on an old snapshot).
    if (std::next(it) == segments_.end()) { ++staleCleanerOps_; return; }

    // Check the CANONICAL <base>.log path, NOT it->second->logPath().  The cleaner
    // thread always reads/writes/deletes the canonical <base>.log; a segment that
    // was pre-provisioned via the append-p99 staging mechanism can still carry a
    // stale staging logPath_ (updated only after drainStagingRenamesDone), so
    // checking logPath_ would see a "missing" staging file and WRONGLY erase a
    // segment the cleaner actually rewrote and KEPT — its data then strands on disk
    // (present under <base>.log) and resurfaces on the next full reloadSegments().
    const std::string logPath = logPathForBase(baseOffset);
    if (!std::filesystem::exists(logPath)) {
        // Cleaner emptied + deleted this segment (or merged it into its
        // predecessor); drop it from the map.  CRITICAL-1: its records may now
        // live in the predecessor (a coalesced merge) — fold its timestamp in
        // there so the merged segment never reads older than its newest member.
        if (it != segments_.begin()) {
            LogSegment& pred = *std::prev(it)->second;
            pred.raiseTsFloor(it->second->publishedTs());
            pred.raiseTsFloor(it->second->tsFloor());
        }
        erase_(it);
        return;
    }

    // Reconstruct just this segment (closes the old fd via unique_ptr reset) and
    // recover() it so its in-memory index reflects the rewritten bytes.
    const bool    wasDurable = it->second->fullyDurable();
    const int64_t oldDurable = it->second->durableNext();
    // CRITICAL-1: a rewrite is a NEW incarnation (fresh lineage from install_);
    // it starts from what the replaced object published (compaction never
    // makes a segment newer; a merge adds its members via raiseTsFloor).
    const int64_t floorTs = std::max(it->second->publishedTs(), it->second->tsFloor());
    install_(baseOffset, std::make_unique<LogSegment>(dir_, baseOffset, kDefaultIndexIntervalBytes, indexMaxBytes_));
    LogSegment& fresh = *segments_[baseOffset];
    fresh.raiseTsFloor(floorTs);
    fresh.recover();
    // E1d: the cleaner fsyncs a rewritten segment before its rename.
    fresh.setDurableNext(wasDurable ? fresh.nextOffset() : std::min(oldDurable, fresh.nextOffset()));
}

int Log::deleteOldSegments(int64_t retainBytes, int64_t highWatermark) {
    if (!beginColdPath_()) return 0;   // fix round 5: posted renames first (retried next pass)
    int deleted = 0;
    int64_t total = totalBytes();   // review LOW-2: one O(n) sum, then a running total
    while (segments_.size() > 1 && total > retainBytes) {
        auto it = segments_.begin();
        // MAJOR-3 (Kafka: HW >= nextSegment.baseOffset): never delete a segment
        // holding records above the HW (not yet replicated / committed).
        if (std::next(it)->first > highWatermark) break;
        std::string base = it->second->logPath();
        std::string idx = indexPathForLog(base);
        total -= it->second->sizeBytes();
        erase_(it);
        std::filesystem::remove(base);
        std::filesystem::remove(idx);
        ++deleted;
    }
    // Log-snapshot review audit: segments_ changed — the cleaner must not keep
    // seeing (and re-targeting) the deleted bases.
    if (deleted > 0) publishSealedSnapshot();
    return deleted;
}

bool Log::cleanerOpApplies_(SegMap::iterator it, uint64_t lineage) noexcept {
    const bool active = std::next(it) == segments_.end();
    if (active || (lineage != kAnyLineage && it->second->tsLineage() != lineage)) {
        ++staleCleanerOps_;   // N4-LOW-3: counted, never logged on the duty pass
        return false;
    }
    return true;
}

bool Log::applyCleanedGroup_(std::size_t first, std::size_t n, int64_t maxBaseBound) {
    // Fix round 4 — the OWNER does NO filesystem operation here.  The cleaner
    // wrote, verified and fsynced the complete output AND opened it (head.
    // prebuilt: fds + index mmap, trusted).  The owner validates metadata,
    // switches its in-memory segments to the prebuilt handle (a later rename
    // never affects an open inode), and posts the file work — the
    // .cleaned → .swap → .log rename sequence + directory fsync, and the
    // destruction (close / munmap) of the retired objects — to the cleaner
    // (runCleanerFileOps).  Crash windows (every one recovers at construction):
    //   - before the cleaner's renames: originals + .cleaned on disk → the
    //     .cleaned is deleted, the uncompacted originals are used (no loss);
    //   - mid-rename: a .swap is completed;  - after: compacted.
    const SegRef head = swapRing_[first & kSwapRingMask];
    const int64_t b1 = head.base;
    auto reject = [&](RejectReason why) {
        ++cleanerGroupsRejected_;
        if (b1 < cleanerRejectLowWater_) cleanerRejectLowWater_ = b1;
        // N4-LOW-3: no logging on the duty pass — the reason travels with the
        // rejection and the cleaner logs it (cold, its own thread).
        CleanerMsg m;
        m.kind = MsgKind::SwapRejected; m.base = b1; m.seq = head.seq; m.n = static_cast<uint16_t>(n);
        m.hasOutput = head.hasOutput; m.obj = head.prebuilt; m.reason = static_cast<uint8_t>(why);
        postMsgs_(&m, 1);   // room was reserved by the caller
        return false;
    };
    // ---- validate (metadata only)
    auto it = segments_.find(b1);
    if (it == segments_.end()) return reject(RejectReason::FrontGone);
    auto cur = it;
    int64_t membersMaxTs = INT64_MIN;
    for (std::size_t k = 0; k < n; ++k, ++cur) {
        const SegRef ref = swapRing_[(first + k) & kSwapRingMask];
        if (cur == segments_.end() || cur->first != ref.base) return reject(RejectReason::NotContiguous);
        if (std::next(cur) == segments_.end()) return reject(RejectReason::Active);
        if (ref.lineage != kAnyLineage && cur->second->tsLineage() != ref.lineage)
            return reject(RejectReason::Lineage);
        if (ref.base >= maxBaseBound) return reject(RejectReason::AboveBound);
        // The cleaner read the CANONICAL <base>.log: a member whose staging→base
        // rename is not yet confirmed (in-memory path still the staging name)
        // may not have been read.  In-memory check (no stat).
        if (cur->second->pathPending()) return reject(RejectReason::RenamePending);
        // The retired object is handed to the cleaner thread: never one with an
        // async flush in flight on its fd (owned by this thread's io backend).
        if (cur->second->flushPins() > 0) return reject(RejectReason::FlushInFlight);
        membersMaxTs = std::max({membersMaxTs, cur->second->publishedTs(), cur->second->tsFloor()});
    }
    const int64_t end = cur->first;   // next segment after the last member (exists: none is active)
    if (!compactionAllowedThrough(end)) return reject(RejectReason::RecoveryPoint);
    if (head.hasOutput && (head.prebuilt == nullptr || head.nextOffset <= b1 || head.nextOffset > end ||
                           head.prebuilt->baseOffset() != b1))
        return reject(RejectReason::BadOutput);

    // ---- accept: switch the in-memory map (no syscall), post the file work.
    std::array<CleanerMsg, 1 + kMaxGroupMembers> msgs;
    msgs[0].kind = MsgKind::SwapAccepted; msgs[0].base = b1; msgs[0].end = end;
    msgs[0].seq = head.seq; msgs[0].n = static_cast<uint16_t>(n); msgs[0].hasOutput = head.hasOutput;
    if (head.hasOutput) {
        const int64_t floorTs = std::max({membersMaxTs, it->second->publishedTs(), it->second->tsFloor()});
        const uint64_t dirGen = it->second->dirEntryGen();
        std::unique_ptr<LogSegment> fresh(head.prebuilt);
        fresh->setStorageState(&storage_);
        fresh->setInstallGen(++segInstallGen_);
        fresh->setTsLineage(fresh->installGen());   // a NEW incarnation (CRITICAL-1)
        fresh->raiseTsFloor(floorTs);               // newest member's timestamp
        fresh->setDirEntryGen(dirGen);
        msgs[1].kind = MsgKind::Dispose; msgs[1].base = b1; msgs[1].obj = it->second.release();
        it->second = std::move(fresh);
        auto m = std::next(it);
        for (std::size_t k = 1; k < n; ++k) {
            msgs[1 + k].kind = MsgKind::Dispose; msgs[1 + k].base = m->first; msgs[1 + k].obj = m->second.release();
            m = segments_.erase(m);
        }
    } else {
        // Every record removed: drop all members (their offsets read as a gap,
        // as with Kafka compaction).  Their timestamps fold into the predecessor.
        if (it != segments_.begin()) std::prev(it)->second->raiseTsFloor(membersMaxTs);
        auto m = it;
        for (std::size_t k = 0; k < n; ++k) {
            msgs[1 + k].kind = MsgKind::Dispose; msgs[1 + k].base = m->first; msgs[1 + k].obj = m->second.release();
            m = segments_.erase(m);
        }
    }
    postMsgs_(msgs.data(), 1 + n);
    return true;
}

bool Log::msgRoom_(std::size_t n) const noexcept {
    const std::size_t tail = msgTail_.load(std::memory_order_relaxed);
    const std::size_t head = msgHead_.load(std::memory_order_acquire);
    return n <= kMsgRingCapacity && tail - head <= kMsgRingCapacity - n;
}

void Log::postMsgs_(const CleanerMsg* m, std::size_t n) noexcept {
    const std::size_t tail = msgTail_.load(std::memory_order_relaxed);
    for (std::size_t k = 0; k < n; ++k) msgRing_[(tail + k) & kMsgRingMask] = m[k];
    msgTail_.store(tail + n, std::memory_order_release);
}

bool Log::applyCleanerMsgs_(int64_t nowMs, bool durable) {
    // The msg ring's consumer (cleaner; ~Log once the cleaner stopped).
    const std::size_t head = msgHead_.load(std::memory_order_relaxed);
    const std::size_t tail = msgTail_.load(std::memory_order_acquire);
    if (head == tail) return false;
    bool dirChanged = false;
    std::size_t i = head;
    // Advance head past every finished message, also on an early stop / throw.
    try {
        dirChanged = applyCleanerMsgRange_(i, tail, nowMs, durable);
    } catch (...) {
        msgHead_.store(i, std::memory_order_release);
        throw;
    }
    msgHead_.store(i, std::memory_order_release);
    return dirChanged;
}

bool Log::applyCleanerMsgRange_(std::size_t& i, std::size_t tail, int64_t nowMs, bool durable) {
    const std::size_t head = i;
    std::error_code ec;
    bool dirChanged = false;
    // 1. A retention drop moves the log start: persist it BEFORE renaming
    //    anything (construction drops what lies below it).  N4-LOW-1: if the
    //    checkpoint cannot be written, NO drop is renamed — stop at the first
    //    one and retry next cycle (the log start never goes backwards), and
    //    tell the owner (it fails the log, Kafka: checkpoint IOException takes
    //    the log dir offline).
    int64_t newStart = -1;
    for (std::size_t k = head; k != tail; ++k) {
        const CleanerMsg& m = msgRing_[k & kMsgRingMask];
        if (m.kind == MsgKind::RetentionDrop && m.logStartAfter > newStart) newStart = m.logStartAfter;
    }
    bool dropsAllowed = true;
    if (durable && newStart > persistedLogStart_ && !writeLogStartCheckpoint_(newStart)) {
        dropsAllowed = false;
        ++checkpointWriteFailures_;
        cleanerFailedErrno_.store(errno != 0 ? errno : EIO, std::memory_order_release);
    }
    auto retire = [&](const std::string& p, const std::string& tag) {
        std::error_code rec;
        std::filesystem::rename(p, p + tag, rec);
        if (!rec) pendingPhysicalDeletes_.emplace_back(p + tag, nowMs);
    };
    while (i != tail) {
        if (failNextCleanerApplyForTest_) [[unlikely]] {
            failNextCleanerApplyForTest_ = false;
            throw std::runtime_error("injected cleaner apply failure (test)");
        }
        const CleanerMsg m = msgRing_[i & kMsgRingMask];
        switch (m.kind) {
        case MsgKind::SwapAccepted: {
            if (durable) {
                const std::string tag = "." + std::to_string(m.seq) + ".deleted";
                if (m.hasOutput) {
                    const std::string cleaned    = cleanedLogPath(dir_, m.base, m.seq);
                    const std::string cleanedIdx = cleanedIndexPath(dir_, m.base, m.seq);
                    const std::string swap       = swapLogPath(dir_, m.base, m.end);
                    std::filesystem::rename(cleaned, swap, ec);                       // COMMIT point
                    for (std::size_t k = 1; k < m.n; ++k) {
                        const int64_t b = msgRing_[(i + 1 + k) & kMsgRingMask].base;
                        retire(logPathForBase(b), tag);
                        std::filesystem::remove(indexPathForBase(b), ec);
                    }
                    retire(logPathForBase(m.base), tag);
                    std::filesystem::rename(cleanedIdx, indexPathForBase(m.base), ec);
                    std::filesystem::rename(swap, logPathForBase(m.base), ec);
                } else {
                    for (std::size_t k = 0; k < m.n; ++k) {
                        const int64_t b = msgRing_[(i + 1 + k) & kMsgRingMask].base;
                        retire(logPathForBase(b), tag);
                        std::filesystem::remove(indexPathForBase(b), ec);
                    }
                }
                dirChanged = true;
            }
            for (std::size_t k = 0; k < m.n; ++k) {
                delete msgRing_[(i + 1 + k) & kMsgRingMask].obj;
                msgRing_[(i + 1 + k) & kMsgRingMask].obj = nullptr;
            }
            i += 1 + m.n;
            break;
        }
        case MsgKind::SwapRejected:
            if (durable && m.hasOutput) {
                std::filesystem::remove(cleanedLogPath(dir_, m.base, m.seq), ec);
                std::filesystem::remove(cleanedIndexPath(dir_, m.base, m.seq), ec);
            }
            // N4-LOW-3: logged HERE (cleaner thread), never on the owner's pass.
            if (rejectLogBudget_ > 0) {
                --rejectLogBudget_;
                spdlog::warn("Log {}: owner rejected the cleaner output for segment {} (+{} member(s)), "
                             "reason {} — original segments untouched", dir_, m.base,
                             m.n > 0 ? m.n - 1 : 0, static_cast<int>(m.reason));
            }
            delete m.obj;
            msgRing_[i & kMsgRingMask].obj = nullptr;
            ++i;
            break;
        case MsgKind::RetentionDrop:
            if (durable) {
                if (!dropsAllowed) return dirChanged;   // retried next cycle (head stays at i)
                retire(logPathForBase(m.base), ".deleted");
                retire(indexPathForBase(m.base), ".deleted");
                dirChanged = true;
            }
            delete m.obj;
            msgRing_[i & kMsgRingMask].obj = nullptr;
            ++i;
            break;
        case MsgKind::Dispose:
        default:
            delete m.obj;
            msgRing_[i & kMsgRingMask].obj = nullptr;
            ++i;
            break;
        }
    }
    return dirChanged;
}

int Log::runCleanerFileOps(int64_t nowMs, int64_t fileDeleteDelayMs) {
    // Cleaner thread (the msg ring's consumer).  No token, no lock: the owner
    // never renames a live name while anything is posted (beginColdPath_).
    bool changed = false;
    try {
        changed = applyCleanerMsgs_(nowMs, /*durable=*/true);
    } catch (const std::exception& ex) {
        // applyCleanerMsgs_ left head at the first unfinished message: retried
        // next cycle.  Nothing is held — the owner is never blocked by this.
        spdlog::error("Log {}: cleaner file operations failed (retried next cycle): {}", dir_, ex.what());
    }
    // Cleaner-only housekeeping — the owner never touches *.deleted names.
    const std::size_t before = pendingPhysicalDeletes_.size();
    physicallyRemoveExpiredDeletedFiles(nowMs, fileDeleteDelayMs);
    const int work = (changed ? 1 : 0) + static_cast<int>(before - pendingPhysicalDeletes_.size());
    if (changed) {
        // Durability of the finished renames.  Crash consistency never depends
        // on it (construction recovers any prefix).
        const int dfd = ::open(dir_.c_str(), O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) { (void)::fsync(dfd); ::close(dfd); }
    }
    return work;
}

bool Log::fileOpsQuiescent() const noexcept {
    const std::size_t tail = msgTail_.load(std::memory_order_relaxed);   // owner = producer
    const std::size_t head = msgHead_.load(std::memory_order_acquire);
    return head == tail;
}

bool Log::cleanerFileOpsPending() const noexcept {
    const std::size_t head = msgHead_.load(std::memory_order_relaxed);   // cleaner = consumer
    const std::size_t tail = msgTail_.load(std::memory_order_acquire);
    return head != tail;
}

bool Log::ownerDrainAllowed_(bool& pause) noexcept {
    // N4-LOW-1: the cleaner could not persist the log start — fail the log
    // (owner thread owns storage_); the drop stays un-renamed (safe).
    if (const int e = cleanerFailedErrno_.load(std::memory_order_acquire); e != 0 && storage_.failedErrno == 0)
        markStorageFailed(e);
    // A waiting cold path needs the cleaner to drain the msg ring: post nothing.
    if (pause) { pause = false; return false; }   // one drain pass paused for a waiting cold path
    return true;
}

bool Log::beginColdPath_() noexcept {
    if (fileOpsQuiescent()) { pauseSwapPost_ = pauseDropPost_ = false; return true; }
    pauseSwapPost_ = pauseDropPost_ = true;   // the next drains post nothing: let the cleaner catch up
    return false;
}

bool Log::writeLogStartCheckpoint_(int64_t v) {
    // Token holder.  12 bytes LE: value i64 | crc32c u32.  tmp + fsync + rename + dir fsync.
    uint8_t buf[12];
    putLe64(buf, static_cast<uint64_t>(v));
    putLe32(buf + 8, rune::protocol::crc32c(buf, 8));
    const std::string tmp = dir_ + "/" + kLogStartTmpFile;
    const std::string fin = dir_ + "/" + kLogStartFile;
    const int fd = ::open(tmp.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) { spdlog::error("Log {}: cannot write {} (errno={})", dir_, kLogStartTmpFile, errno); return false; }
    const bool ok = ::write(fd, buf, sizeof(buf)) == static_cast<ssize_t>(sizeof(buf)) && ::fsync(fd) == 0;
    ::close(fd);
    if (!ok || ::rename(tmp.c_str(), fin.c_str()) != 0) {
        spdlog::error("Log {}: log-start checkpoint write failed (errno={})", dir_, errno);
        return false;
    }
    const int dfd = ::open(dir_.c_str(), O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) { (void)::fsync(dfd); ::close(dfd); }
    persistedLogStart_ = v;
    return true;
}

int64_t Log::readLogStartCheckpoint(const std::string& dir) {
    uint8_t buf[12];
    const std::string p = dir + "/" + kLogStartFile;
    const int fd = ::open(p.c_str(), O_RDONLY);
    if (fd < 0) return 0;
    const ssize_t n = ::read(fd, buf, sizeof(buf));
    ::close(fd);
    if (n != static_cast<ssize_t>(sizeof(buf)) || getLe32(buf + 8) != rune::protocol::crc32c(buf, 8)) return 0;
    const int64_t v = static_cast<int64_t>(getLe64(buf));
    return v > 0 ? v : 0;
}

std::unique_ptr<LogSegment> Log::openCleanedOutput(int64_t base, uint64_t seq, int64_t nextOffset) const {
    return LogSegment::openPrebuilt(dir_, base, cleanedLogPath(dir_, base, seq), cleanedIndexPath(dir_, base, seq),
                                    nextOffset, kDefaultIndexIntervalBytes, indexMaxBytes_);
}

void Log::recoverCleanerSwapFiles() {
    // Fix round 2 crash recovery (construction, before segment discovery).
    //   *.cleaned : an UNCOMMITTED cleaner output — delete (originals intact).
    //   *.swap    : a COMMITTED swap <b1>.log.<end>.swap replacing [b1, end) —
    //               delete every remaining segment in (b1, end) and b1's index,
    //               then rename it over <b1>.log (idempotent if re-run).
    std::vector<std::filesystem::path> cleaned, swaps;
    for (auto& e : std::filesystem::directory_iterator(dir_)) {
        const auto ext = e.path().extension();
        if (ext == ".cleaned") cleaned.push_back(e.path());
        else if (ext == ".swap") swaps.push_back(e.path());
    }
    std::error_code ec;
    for (const auto& p : cleaned) {
        spdlog::warn("Log {}: removing uncommitted cleaner output {}", dir_, p.filename().string());
        std::filesystem::remove(p, ec);
    }
    for (const auto& p : swaps) {
        // <base20>.log.<end>.swap
        const std::string name = p.filename().string();
        const auto dot1 = name.find(".log.");
        const auto dot2 = name.rfind(".swap");
        if (dot1 == std::string::npos || dot2 == std::string::npos || dot2 <= dot1 + 5) continue;
        int64_t b1 = 0, end = 0;
        try {
            b1  = std::stoll(name.substr(0, dot1));
            end = std::stoll(name.substr(dot1 + 5, dot2 - dot1 - 5));
        } catch (...) { continue; }
        spdlog::warn("Log {}: completing committed compaction swap {} (segments [{}, {}))",
                     dir_, name, b1, end);
        for (auto& e : std::filesystem::directory_iterator(dir_)) {
            if (e.path().extension() != ".log") continue;
            int64_t b = 0;
            try { b = std::stoll(e.path().stem().string()); } catch (...) { continue; }
            if (b > b1 && b < end) {
                std::filesystem::remove(e.path(), ec);
                std::filesystem::remove(indexPathForBase(b), ec);
            }
        }
        std::filesystem::remove(indexPathForBase(b1), ec);
        std::filesystem::rename(p, logPathForBase(b1), ec);
        if (ec) fatalHalt("Log: cannot complete compaction swap " + p.string() + ": " + ec.message());
    }
}

bool Log::cleanerSwapsPending() const noexcept {
    // Cleaner thread (the swap ring's producer): own tail vs the consumer head.
    const std::size_t tail = swapTail_.load(std::memory_order_relaxed);
    const std::size_t head = swapHead_.load(std::memory_order_acquire);
    return tail != head;
}

bool Log::enqueueSwapSlots_(const SegRef* refs, std::size_t n) noexcept {
    // LOCK-FREE SPSC producer (single cleaner thread).  No mutex on any path.
    // Uses FREE-RUNNING (unmasked) monotonic counters: head_/tail_ only ever
    // increase; the index into swapRing_ is (counter & kSwapRingMask).  Comparing
    // the raw counters gives an unambiguous occupancy (tail - head) and lets the
    // ring use its full capacity (no wasted slot).
    //
    // tail_ is producer-owned: a relaxed load reads our own last position.
    // head_ is consumer-owned: an acquire-load observes slots freed by the append
    // thread's release-store of head_ in drainCleanerSwaps().
    const std::size_t tail = swapTail_.load(std::memory_order_relaxed);
    const std::size_t head = swapHead_.load(std::memory_order_acquire);
    if (n == 0 || n > kSwapRingCapacity || tail - head > kSwapRingCapacity - n) {
        // Ring full — drop (return false).  The cleaner retries next cycle.
        return false;
    }
    for (std::size_t k = 0; k < n; ++k) swapRing_[(tail + k) & kSwapRingMask] = refs[k];
    // ONE release-store publishes every slot of the group (the consumer sees all
    // of it or none) AND the cleaner's prior durable output write.
    swapTail_.store(tail + n, std::memory_order_release);
    return true;
}

bool Log::enqueueCleanedSegment(int64_t base, uint64_t lineage) noexcept {
    // Legacy in-place reload (a synchronous / test caller already replaced the file).
    const SegRef ref{base, lineage, 0, 0, 0, false, nullptr};
    return enqueueSwapSlots_(&ref, 1);
}

bool Log::enqueueCleanedGroup(const CleanedMember* members, std::size_t n, uint64_t seq,
                              bool hasOutput, int64_t nextOffset, LogSegment* prebuilt) noexcept {
    // Fix round 2: a deferred compaction output.  Bounded by the cleaner
    // (LogCleaner::kMaxDeferredGroupMembers); the slots live in a fixed scratch
    // array — no allocation on the cleaner side either.
    constexpr std::size_t kMax = kMaxGroupMembers;
    if (n == 0 || n > kMax) return false;
    std::array<SegRef, kMax> refs;
    for (std::size_t k = 0; k < n; ++k)
        refs[k] = SegRef{members[k].base, members[k].lineage, k == 0 ? nextOffset : 0,
                         static_cast<uint32_t>(seq), static_cast<uint16_t>(k == 0 ? n : 0), hasOutput,
                         k == 0 ? prebuilt : nullptr};
    // groupSize 0 marks a legacy entry: non-first members are never read alone
    // (the drain consumes the whole group from its first slot).
    return enqueueSwapSlots_(refs.data(), n);
}

std::string Log::cleanedLogPath(const std::string& dir, int64_t base, uint64_t seq) {
    std::string s = std::to_string(base);
    if (s.size() < 20) s = std::string(20 - s.size(), '0') + s;
    return dir + "/" + s + ".log." + std::to_string(static_cast<uint32_t>(seq)) + ".cleaned";
}
std::string Log::cleanedIndexPath(const std::string& dir, int64_t base, uint64_t seq) {
    std::string s = std::to_string(base);
    if (s.size() < 20) s = std::string(20 - s.size(), '0') + s;
    return dir + "/" + s + ".index." + std::to_string(static_cast<uint32_t>(seq)) + ".cleaned";
}
std::string Log::swapLogPath(const std::string& dir, int64_t base, int64_t end) {
    std::string s = std::to_string(base);
    if (s.size() < 20) s = std::string(20 - s.size(), '0') + s;
    return dir + "/" + s + ".log." + std::to_string(end) + ".swap";
}

int Log::drainCleanerSwaps(int64_t maxBaseBound) {
    // LOCK-FREE SPSC consumer (single append/duty-cycle thread).  No mutex.
    // head_ is consumer-owned: relaxed load of our own position.
    // tail_ acquire-load pairs with the producer's release-store — the acquire
    // half of the happens-before between the cleaner's file rename and the
    // reloadSegment() calls below.
    const std::size_t head = swapHead_.load(std::memory_order_relaxed);
    const std::size_t tail = swapTail_.load(std::memory_order_acquire);
    if (!ownerDrainAllowed_(pauseSwapPost_) || head == tail) return 0;   // fix round 5: cold path waiting / log failed
    // In-memory only: confirm staging→base renames so the canonical-name check
    // in applyCleanedGroup_ is exact.
    (void)drainStagingRenamesDone();
    return drainSwapSlots_(head, tail, maxBaseBound);
}

int Log::drainSwapSlots_(std::size_t head, std::size_t tail, int64_t maxBaseBound) {
    int groupsApplied = 0;
    for (std::size_t i = head; i != tail; ) {
        const SegRef ref = swapRing_[i & kSwapRingMask];
        if (ref.groupSize > 0) {
            // Fix round 2: a deferred group — the producer published all of its
            // slots with one release-store, so [i, i + groupSize) is complete.
            // Fix round 3: at most ONE group per call (= per partition per duty
            // pass): the rest stay queued for the next pass.
            if (groupsApplied > 0) { tail = i; break; }
            // Fix round 4: the outcome is posted to the cleaner — leave the group
            // queued if the message ring has no room (never block / drop).
            if (!msgRoom_(1 + static_cast<std::size_t>(ref.groupSize))) { tail = i; break; }
            (void)applyCleanedGroup_(i, ref.groupSize, maxBaseBound);
            ++groupsApplied;
            i += ref.groupSize;
            continue;
        }
        ++i;
        auto it = segments_.find(ref.base);
        if (it == segments_.end()) continue;               // already gone: idempotent
        if (!cleanerOpApplies_(it, ref.lineage)) continue;  // MAJOR-2: stale swap
        reloadSegmentNoPublish_(ref.base);
    }
    const int count = static_cast<int>(tail - head);
    if (count > 0) {
        // Release new head so the cleaner can reuse those ring slots.
        swapHead_.store(tail, std::memory_order_release);
        // segments_ changed (segment reloaded or dropped); refresh the snapshot
        // so the cleaner thread sees correct sizes on the next scan.
        publishSealedSnapshot();
    }
    return count;
}

// ---------------------------------------------------------------------------
// CP-9: Retention async-delete — SPSC ring for deletion enqueues.
// ---------------------------------------------------------------------------
bool Log::enqueueDeletedSegment(int64_t base, uint64_t lineage) noexcept {
    // LOCK-FREE SPSC producer (cleaner thread only).
    // Mirror of enqueueCleanedSegment but for the delete ring.
    const std::size_t tail = deleteTail_.load(std::memory_order_relaxed);
    const std::size_t head = deleteHead_.load(std::memory_order_acquire);
    if (tail - head >= kDeleteRingCapacity) {
        // Ring full — caller retries next cycle.
        return false;
    }
    deleteRing_[tail & kDeleteRingMask] = SegRef{base, lineage};
    // Release-store: publishes the base AND the prior .log→.deleted file rename
    // to the consumer's acquire-load in drainRetentionDeletes.
    deleteTail_.store(tail + 1, std::memory_order_release);
    return true;
}

int Log::drainRetentionDeletes(int64_t maxEndBound) {
    // LOCK-FREE SPSC consumer (owner thread only).
    // Fix round 4: NO filesystem operation here.  The cleaner only PROPOSED
    // (base, lineage).  The owner validates — the segment exists, is not the
    // active one, is the same incarnation, at its canonical name, has no flush
    // in flight, and ENDS at or below the HW — then removes it from segments_
    // and posts it to the cleaner, which persists the new log start, renames
    // the files to *.deleted and destroys the object (close / munmap).
    // Crash windows: before the cleaner's checkpoint the segment simply
    // reappears (retention re-runs; nothing lost); after it, construction drops
    // every segment below the persisted log start.
    const std::size_t head = deleteHead_.load(std::memory_order_relaxed);
    const std::size_t tail = deleteTail_.load(std::memory_order_acquire);
    if (!ownerDrainAllowed_(pauseDropPost_) || head == tail) return 0;   // fix round 5: cold path waiting / log failed
    (void)drainStagingRenamesDone();   // in-memory: exact canonical-name check
    int count = 0;
    std::size_t i = head;
    for (; i != tail; ++i) {
        const SegRef ref = deleteRing_[i & kDeleteRingMask];
        auto it = segments_.find(ref.base);
        if (it == segments_.end()) continue;                   // already gone: idempotent
        // N4-LOW-2 (Kafka): retention deletes only a PREFIX — never a segment
        // with a live one below it (no offset hole below the HW).  A proposal
        // behind a skipped one is dropped; the cleaner re-proposes.
        if (it != segments_.begin()) { ++staleCleanerOps_; continue; }
        if (!cleanerOpApplies_(it, ref.lineage)) continue;      // stale / active
        if (std::next(it)->first > maxEndBound) { ++staleCleanerOps_; continue; }   // MAJOR-3
        if (it->second->pathPending() || it->second->flushPins() > 0) {
            ++staleCleanerOps_;
            continue;   // re-proposed next retention cycle
        }
        if (!msgRoom_(1)) break;   // leave the rest queued (never block)
        CleanerMsg m;
        m.kind = MsgKind::RetentionDrop;
        m.base = ref.base;
        m.obj  = it->second.release();
        segments_.erase(it);
        m.logStartAfter = segments_.begin()->first;
        postMsgs_(&m, 1);
        ++count;
    }
    // Log-snapshot review MEDIUM-1: ALWAYS release the consumed slots (mirror
    // drainCleanerSwaps) — gating this on count > 0 leaked slots for bases that
    // were already gone until the ring filled and retention halted forever.
    if (i != head) {
        deleteHead_.store(i, std::memory_order_release);
    }
    // Refresh the sealed snapshot so the cleaner sees the updated segment list.
    if (count > 0) publishSealedSnapshot();
    return count;
}

void Log::discoverDeletedFiles(int64_t nowMs) {
    // Cleaner thread: the owner renames retention victims to *.deleted; schedule
    // each newly seen one for physical removal after file.delete.delay.  Cold
    // (once per retention cycle); the pending list is small.
    std::error_code ec;
    for (auto& e : std::filesystem::directory_iterator(dir_, ec)) {
        if (e.path().extension() != ".deleted") continue;
        const std::string p = e.path().string();
        bool known = false;
        for (const auto& pd : pendingPhysicalDeletes_) if (pd.first == p) { known = true; break; }
        if (!known) pendingPhysicalDeletes_.emplace_back(p, nowMs);
    }
}

void Log::recordDeletedFile(const std::string& deletedPath, int64_t renamedAtMs) noexcept {
    // Called only on the cleaner thread — no synchronization needed.
    pendingPhysicalDeletes_.emplace_back(deletedPath, renamedAtMs);
}

void Log::physicallyRemoveExpiredDeletedFiles(int64_t nowMs, int64_t fileDeleteDelayMs) noexcept {
    // Called only on the cleaner thread — safe to iterate and modify.  Every
    // *.deleted file (<base>.log.deleted AND <base>.index.deleted — the owner
    // renames both) is its own entry: nothing is derived from a path, so a live
    // segment's .index is never touched (fix round 3).
    auto it = pendingPhysicalDeletes_.begin();
    while (it != pendingPhysicalDeletes_.end()) {
        if (nowMs - it->second >= fileDeleteDelayMs) {
            std::error_code ec;
            std::filesystem::remove(it->first, ec);
            it = pendingPhysicalDeletes_.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace rune::storage
