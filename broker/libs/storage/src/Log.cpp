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
    // E1d: clean shutdown persists the exact recovery point (a stale, lower one
    // would only make the next recovery validate more).  Never on a failed log.
    if (recovered_ && !crashedForTest_ && storage_.failedErrno == 0 &&
        (recoveryPoint_ != persistedRp_ || rpUnconfirmedMax_ > recoveryPoint_)) {
        std::error_code ec;
        if (std::filesystem::is_directory(dir_, ec)) (void)checkpointRecoveryPoint();   // dir removed: nothing to persist
    }
    if (rpFd_ >= 0) { ::close(rpFd_); rpFd_ = -1; }
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
        int64_t base = logEndOffset_;
        // Install the NEXT segment.  Adopts the background-pre-provisioned staging
        // segment (ZERO syscalls: in-memory rebase; the staging→<base> disk rename
        // is deferred to the bg thread) when ready; otherwise falls back to a
        // synchronous create.  The seal fsync above is unchanged and fully
        // synchronous (durability preserved).
        installNextSegment(base);
        // #5 non-blocking append: preallocate the freshly-installed active segment's
        // blocks (negligible vs the seal fsync above) so its hot-path appends don't
        // stall on block allocation.
        activeSegment().preallocate(rollBytes_);
        // CP-8 FIX (incremental largestTimestamp): capture the running max we
        // tracked for the about-to-be-sealed segment, then reset for the new
        // active segment.  publishSealedSnapshot will use this captured value
        // (via newlySealedMaxTs_) instead of re-scanning the file.
        newlySealedMaxTs_ = activeSegmentMaxTs_;
        activeSegmentMaxTs_ = 0;
        // E1d: an inline seal made the sealed segment durable — R may cross into
        // the new active segment (persisted inline on the legacy path only).
        advanceRecoveryPoint_();
        maybeCheckpointInline_();
        // Publish snapshot: a new sealed segment just appeared.
        publishSealedSnapshot();
        // Clear the seal-time hint after the snapshot consumed it.
        newlySealedMaxTs_ = 0;
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
        auto it  = segments_.begin();
        auto end = std::prev(segments_.end()); // stop before active
        for (; it != end; ++it) {
            int64_t base  = it->first;
            int64_t bytes = it->second->sizeBytes();
            // Exclusive end = next segment's base (the active segment's base for the
            // last sealed entry).  std::next(it) is always valid here (it != end,
            // and end == the active segment).
            int64_t endOff = std::next(it)->first;
            // Try to reuse largestTimestamp from the previous snapshot for this
            // segment (sealed segments are immutable after they are created).
            int64_t largestTs = 0;
            if (prevSnap) {
                for (const auto& si : *prevSnap) {
                    if (si.baseOffset == base) { largestTs = si.largestTimestamp; break; }
                }
            }
            // If not found in the previous snapshot, this is either:
            //   (a) A NEWLY SEALED segment from maybeRoll(): use newlySealedMaxTs_
            //       (CP-8 fix — zero append-path I/O at seal time).
            //   (b) A segment recovered from disk by recover(): use recoveryMaxTimestamps_
            //       (CP-5.1 fix — real timestamps to prevent retention-after-restart data loss).
            if (largestTs == 0) {
                if (newlySealedMaxTs_ != 0) {
                    largestTs = newlySealedMaxTs_;
                } else {
                    // CP-5.1: check the recovery map (populated by recover() batch scan).
                    auto rmIt = recoveryMaxTimestamps_.find(base);
                    if (rmIt != recoveryMaxTimestamps_.end()) {
                        largestTs = rmIt->second;
                    }
                }
            }
            snap->push_back({base, bytes, largestTs, endOff, compactionAllowedThrough(endOff)});
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
        rune::protocol::RecordBatchReader r(data, len);
        int64_t batchMaxTs = toEpochMs(r.maxTimestamp());   // ns- or ms-stamped -> epoch ms
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
        rune::protocol::RecordBatchReader r(data, len);
        int64_t batchMaxTs = toEpochMs(r.maxTimestamp());   // ns- or ms-stamped -> epoch ms
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
    logEndOffset_ = activeSegment().appendNoSyncRestamp(batch, len, base, lastOffset);
    if (len >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
        rune::protocol::RecordBatchReader r(batch, len);
        int64_t batchMaxTs = toEpochMs(r.maxTimestamp());   // ns- or ms-stamped -> epoch ms
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
            int64_t base = it->first;
            const std::string& logPath = it->second->logPath();
            // Read the segment file and scan all batches for the max maxTimestamp.
            std::ifstream f(logPath, std::ios::binary);
            if (!f.is_open()) continue;
            f.seekg(0, std::ios::end);
            std::streamsize fileSize = f.tellg();
            if (fileSize <= 0) continue;
            f.seekg(0, std::ios::beg);
            std::vector<uint8_t> buf(static_cast<std::size_t>(fileSize));
            f.read(reinterpret_cast<char*>(buf.data()), fileSize);
            f.close();

            int64_t segMaxTs = 0;
            std::size_t pos = 0;
            while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= buf.size()) {
                rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
                int32_t total = r.totalLength();
                if (total <= 0 || pos + static_cast<std::size_t>(total) > buf.size()) break;
                if (!r.ensureValid()) break;
                int64_t batchMaxTs = toEpochMs(r.maxTimestamp());   // ns- or ms-stamped -> epoch ms
                if (batchMaxTs > segMaxTs) segMaxTs = batchMaxTs;
                pos += static_cast<std::size_t>(total);
            }
            if (segMaxTs > 0) {
                recoveryMaxTimestamps_[base] = segMaxTs;
            }
        }
    }

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
        if (deleted) {   // the deletions themselves: a FULL directory fsync (cold)
            const int dfd = ::open(dir_.c_str(), O_RDONLY | O_DIRECTORY);
            ++storage_.roleChangeSyncs;
            if (dfd < 0 || ::fsync(dfd) != 0) {
                if (storage_.failedErrno == 0) storage_.failedErrno = errno != 0 ? errno : EIO;
            }
            if (dfd >= 0) ::close(dfd);
        }
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
    while (it != segments_.end()) {
        LogSegment& seg = *it->second;
        if (seg.durableNext() > r) r = seg.durableNext();
        auto nx = std::next(it);
        if (nx == segments_.end() || !seg.fullyDurable()) break;
        if (nx->first > r) r = nx->first;   // a sealed segment fully durable: R reaches the next base
        it = nx;
    }
    if (r <= recoveryPoint_) return;
    recoveryPoint_ = r;
    if (!rpDue_) refreshRpDue_();
}

void Log::maybeCheckpointInline_() noexcept {
    if (rpDue_ && !deferSealSync_ && recovered_ && storage_.failedErrno == 0 &&
        rp_.stage == RpStage::Idle)
        (void)checkpointRecoveryPoint();
}

bool Log::noteFlushCompleted(LogSegment* seg, int64_t upTo) noexcept {
    if (seg == nullptr) return false;
    seg->markDurableThrough(upTo);
    advanceRecoveryPoint_();
    return recoveryCheckpointDue();
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
    return recoveryCheckpointDue();
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
    auto it = segments_.find(base);
    if (it != segments_.end()) {
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
void Log::truncateTo(int64_t truncateOffset) {
    if (truncateOffset >= logEndOffset_) return; // nothing to do
    // E1d F6: the lowered recovery point is DURABLE before any file is deleted
    // or resized (a crash in between must never trust bytes about to go).
    persistAtMost_(std::max<int64_t>(0, truncateOffset));
    // Fail-stop: a failed persist leaves every file untouched (the log is
    // permanently failed; its owner halts the broker; restart recovers).
    if (recovered_ && storage_.failedErrno != 0) [[unlikely]] return;
    // F30 fix round 1: the owner's flush bookkeeping must learn the LOWEST
    // point truncated to, even when new bytes are re-appended before it looks
    // (LEO alone hides that).  Recorded now (an exception below still lowered
    // the content) and again with the exact resulting LEO at each exit.
    noteTruncatedTo_(std::max<int64_t>(0, truncateOffset));
    ++truncations_;

    // Remove segments that start at or beyond truncateOffset.
    auto it = segments_.lower_bound(truncateOffset);
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
        logEndOffset_ = base;
        noteTruncatedTo_(logEndOffset_);
        persistAtMost_(logEndOffset_);   // E1d (batch-rounded LEO may be lower)
        return;
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
    noteTruncatedTo_(logEndOffset_);   // rounded down to the batch boundary
    persistAtMost_(logEndOffset_);   // E1d: a mid-batch cut rounds the LEO below truncateOffset
}

void Log::reloadSegments() {
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
}

void Log::reloadSegment(int64_t baseOffset) {
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

    // Check the CANONICAL <base>.log path, NOT it->second->logPath().  The cleaner
    // thread always reads/writes/deletes the canonical <base>.log; a segment that
    // was pre-provisioned via the append-p99 staging mechanism can still carry a
    // stale staging logPath_ (updated only after drainStagingRenamesDone), so
    // checking logPath_ would see a "missing" staging file and WRONGLY erase a
    // segment the cleaner actually rewrote and KEPT — its data then strands on disk
    // (present under <base>.log) and resurfaces on the next full reloadSegments().
    const std::string logPath = logPathForBase(baseOffset);
    if (!std::filesystem::exists(logPath)) {
        // Cleaner emptied + deleted this segment; drop it from the map.
        erase_(it);
        return;
    }

    // Reconstruct just this segment (closes the old fd via unique_ptr reset) and
    // recover() it so its in-memory index reflects the rewritten bytes.
    const bool    wasDurable = it->second->fullyDurable();
    const int64_t oldDurable = it->second->durableNext();
    install_(baseOffset, std::make_unique<LogSegment>(dir_, baseOffset, kDefaultIndexIntervalBytes, indexMaxBytes_));
    LogSegment& fresh = *segments_[baseOffset];
    fresh.recover();
    // E1d: the cleaner fsyncs a rewritten segment before its rename.
    fresh.setDurableNext(wasDurable ? fresh.nextOffset() : std::min(oldDurable, fresh.nextOffset()));
}

int Log::deleteOldSegments(int64_t retainBytes) {
    int deleted = 0;
    while (segments_.size() > 1 && totalBytes() > retainBytes) {
        auto it = segments_.begin();
        std::string base = it->second->logPath();
        std::string idx = indexPathForLog(base);
        erase_(it);
        std::filesystem::remove(base);
        std::filesystem::remove(idx);
        ++deleted;
    }
    return deleted;
}

bool Log::enqueueCleanedSegment(int64_t base) noexcept {
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
    if (tail - head >= kSwapRingCapacity) {
        // Ring full — drop (return false).  The cleaner re-enqueues the same base
        // next cycle; reloadSegment is idempotent so a delayed reload is safe.
        return false;
    }
    swapRing_[tail & kSwapRingMask] = base;
    // Release-store publishes the slot AND the cleaner's prior on-disk rename to
    // the consumer's acquire-load of tail_ in drainCleanerSwaps() — the barrier
    // between the cleaner's file rename and the append thread's reloadSegment().
    swapTail_.store(tail + 1, std::memory_order_release);
    return true;
}

int Log::drainCleanerSwaps() {
    // LOCK-FREE SPSC consumer (single append/duty-cycle thread).  No mutex.
    // head_ is consumer-owned: relaxed load of our own position.
    // tail_ acquire-load pairs with the producer's release-store — the acquire
    // half of the happens-before between the cleaner's file rename and the
    // reloadSegment() calls below.
    const std::size_t head = swapHead_.load(std::memory_order_relaxed);
    const std::size_t tail = swapTail_.load(std::memory_order_acquire);
    for (std::size_t i = head; i != tail; ++i) {
        reloadSegment(swapRing_[i & kSwapRingMask]);
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
bool Log::enqueueDeletedSegment(int64_t base) noexcept {
    // LOCK-FREE SPSC producer (cleaner thread only).
    // Mirror of enqueueCleanedSegment but for the delete ring.
    const std::size_t tail = deleteTail_.load(std::memory_order_relaxed);
    const std::size_t head = deleteHead_.load(std::memory_order_acquire);
    if (tail - head >= kDeleteRingCapacity) {
        // Ring full — caller retries next cycle.
        return false;
    }
    deleteRing_[tail & kDeleteRingMask] = base;
    // Release-store: publishes the base AND the prior .log→.deleted file rename
    // to the consumer's acquire-load in drainRetentionDeletes.
    deleteTail_.store(tail + 1, std::memory_order_release);
    return true;
}

int Log::drainRetentionDeletes() {
    // LOCK-FREE SPSC consumer (append/duty-cycle thread only).
    // For each enqueued base, erase the segment from segments_.
    // This is the ONLY place segments_ is mutated for retention (single-writer invariant).
    const std::size_t head = deleteHead_.load(std::memory_order_relaxed);
    const std::size_t tail = deleteTail_.load(std::memory_order_acquire);
    int count = 0;
    for (std::size_t i = head; i != tail; ++i) {
        const int64_t base = deleteRing_[i & kDeleteRingMask];
        auto it = segments_.find(base);
        if (it != segments_.end()) {
            // Erase the in-memory segment (closes the file descriptor).
            // The .log file is already renamed to .deleted on disk; no file I/O here.
            erase_(it);
            ++count;
        }
        // If base is not in segments_ (e.g. idempotent retry), that is fine.
    }
    if (count > 0) {
        // Release new head so the cleaner can reuse slots.
        deleteHead_.store(tail, std::memory_order_release);
        // Refresh the sealed snapshot so the cleaner sees the updated segment list.
        publishSealedSnapshot();
    }
    return count;
}

void Log::recordDeletedFile(const std::string& deletedPath, int64_t renamedAtMs) noexcept {
    // Called only on the cleaner thread — no synchronization needed.
    pendingPhysicalDeletes_.emplace_back(deletedPath, renamedAtMs);
}

void Log::physicallyRemoveExpiredDeletedFiles(int64_t nowMs, int64_t fileDeleteDelayMs) noexcept {
    // Called only on the cleaner thread — safe to iterate and modify.
    // The .deleted path is <dir>/<base20>.log.deleted.
    // Derive the .index path as <dir>/<base20>.index.
    static const std::string kDeletedSuffix = ".deleted";
    static const std::string kLogSuffix     = ".log";
    auto it = pendingPhysicalDeletes_.begin();
    while (it != pendingPhysicalDeletes_.end()) {
        const int64_t age = nowMs - it->second;
        if (age >= fileDeleteDelayMs) {
            // Delay elapsed: physically remove the .deleted file.
            std::filesystem::remove(it->first);
            // Also remove the .index companion.
            // Path: <stem>.log.deleted → strip ".deleted" → <stem>.log → strip ".log" → <stem>.index
            const std::string& deletedPath = it->first;
            if (deletedPath.size() > kDeletedSuffix.size()) {
                // Strip ".deleted" suffix → original .log path.
                std::string logPath = deletedPath.substr(
                    0, deletedPath.size() - kDeletedSuffix.size());
                if (logPath.size() > kLogSuffix.size() &&
                    logPath.substr(logPath.size() - kLogSuffix.size()) == kLogSuffix) {
                    // Strip ".log" → base path, add ".index".
                    std::string indexPath =
                        logPath.substr(0, logPath.size() - kLogSuffix.size()) + ".index";
                    std::filesystem::remove(indexPath);
                }
            }
            it = pendingPhysicalDeletes_.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace rune::storage
