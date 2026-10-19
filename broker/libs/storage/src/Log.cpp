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
#include <charconv>
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

Log::Log(std::string dir, int64_t segmentRollBytes, unsigned fileMode)
    : dir_(std::move(dir)), rollBytes_(segmentRollBytes), fileMode_(fileMode) {
    // Review m2: sized for the deferred-roll overshoot cap, so a segment that
    // overshot rollBytes never grows its index mapping on the owner.
    indexMaxBytes_ = indexMaxBytesFor(maxDeferredRollBytes_(), kDefaultIndexIntervalBytes);
    spareSegNodes_.reserve(kSpareSegNodes);   // hotpath-A: recycled map nodes (see install_)
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
        if (rf.state == RpFileState::Valid) setPersistedRp_(rf.r);
    }
    // F3: eager read-map ONLY the LAST (highest-base) segment — it becomes the
    // active segment.  Earlier (sealed) segments map their .log lazily on first
    // readView(), so N retained segments no longer means N read mmaps + N fds
    // kept alive for the whole process lifetime.
    for (int64_t b : bases)
        install_(b, std::make_unique<LogSegment>(dir_, b, kDefaultIndexIntervalBytes,
                                                  indexMaxBytes_, /*prefaultIndex*/false,
                                                  /*eagerReadMap*/ b == bases.back(),
                                                  /*readMapBytes*/0, fileMode_));
    logEndOffset_ = activeSegment().nextOffset();
    // #5 non-blocking append: preallocate the active segment's blocks so hot-path
    // O_APPEND pwrite()s never stall on block allocation (append-p99 tail).
    activeSegment().preallocate(rollBytes_);
}
Log::~Log() {
    // Review Md5: grown mappings never installed (the provisioner is stopped).
    (void)mapRing_.poll([](int32_t type, int64_t a, int64_t) {
        if (type == kMapGrownMsg) delete reinterpret_cast<MappedFile*>(static_cast<std::uintptr_t>(a));
    }, 1 << 20);
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
    // A simulated CRASH (crashForTest) leaves the renames and the ready
    // staging files exactly as a real crash would (construction reconciles).
    if (!crashedForTest_) {
        const std::size_t head = renameReqHead_.load(std::memory_order_relaxed);
        const std::size_t tail = renameReqTail_.load(std::memory_order_relaxed);
        for (std::size_t i = head; i != tail; ++i) {
            const auto rr = renameReqRing_[i & kRenameRingMask];
            renameStagingToBase(rr.seq, rr.base);
        }
    }
    // hotpath-A F2: free every ready-but-unconsumed segment (the provisioner is
    // stopped: this thread is the ring's only party now).  Its destructor
    // closes fds / unmaps; then its never-written staging files are removed.
    failFront_ = 0;
    for (LogSegment* ready = takeReady_(); ready != nullptr || failFront_ != 0; ready = takeReady_()) {
        failFront_ = 0;   // failure records hold nothing
        if (ready == nullptr) continue;
        const int64_t seq = ready->stagingSeq();
        delete ready;
        if (crashedForTest_) continue;
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
    // A segment can therefore exceed rollBytes by at most one batch — or, while
    // a roll is DEFERRED (hotpath-A, below), by the appends that arrive before
    // the condition clears.  A single oversized batch (larger than rollBytes)
    // always starts a new segment of its own — no splitting occurs.
    const int64_t activeBytes = activeSegment().sizeBytes();
    if (activeBytes < rollBytes_) [[likely]] {
        // Review M3: demand the next segment once the active one is
        // provisionThresholdBytes_ full (one compare per append otherwise).
        if (provisioned_ && activeBytes >= provisionThresholdBytes_ && (!thresholdPosted_ || adoptedPostPending_))
            [[unlikely]] postProvisionDemands_(activeBytes);
        // Review Md5: request the full-segment read mapping off the owner at
        // half full — D26: only for a log that has a mapped reader (a push-only
        // log's answer was never polled: a dead off-owner mmap).
        if (asyncReadGrowth_ && mapReaders_ && !growOutstanding_ &&
            static_cast<std::size_t>(activeBytes) * 2 > activeSegment().readMapBytes()) [[unlikely]] {
            auto last = std::prev(segments_.end());
            maybeGrowReadMap_(*last->second, last->first, true);
        }
        return;
    }
    // hotpath-A F2 + F3: a roll NEVER blocks the owner.  No ready segment on a
    // provisioned log (never an inline create), or the async seal queue full
    // (never an inline seal fsync on a worker-driven log): defer the roll — the
    // batch lands in the active segment; the intake back-pressures on
    // rollDeferred(); the first append after the provisioner / io backend
    // caught up rolls.
    if (rollBlockedNow_()) [[unlikely]] {
        // Correctness backstop: a deferral never lets a segment grow without
        // bound (positions are 32-bit; a stuck provisioner / io backend must
        // not wedge the log) — past the cap the roll is forced, blocking but
        // correct (inline create / inline seal fsync, the pre-hotpath-A path).
        if (activeSegment().sizeBytes() < maxDeferredRollBytes_()) [[likely]] {
            ++rollDeferrals_;
            return;
        }
        ++forcedRolls_;
    }
    // B1 fix (fsync-on-seal): the sealed segment is made durable exactly once.
    if (deferSealSync_ && pendingSealCount_ < kMaxPendingSeals) {
        // Shard mode (spec §3.1): no inline fsync on the loop.  Pin the
        // sealed segment (its fd must outlive the async flush — M1) and queue
        // it (room checked above); the owning worker fdatasyncs it through its
        // io backend, and Strict acks on this log wait until every pending
        // seal has completed.  (D20: writes are unbuffered pwrites — nothing
        // to push to the page cache first.)
        LogSegment& sealed = activeSegment();
        sealed.pinFlush();
        pendingSeals_[pendingSealCount_++] = &sealed;
    } else {
        // Legacy (control-thread logs), or a FORCED roll with the seal queue
        // full (see above): inline seal fsync.
        activeSegment().sync();
    }
    const int64_t base = logEndOffset_;
    // Install the NEXT segment: adopt the oldest ready (pre-provisioned) one —
    // ZERO syscalls, its blocks already fallocated by the provisioner (F1);
    // legacy logs fall back to a synchronous create when the pool is empty.
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
    // CP-8 FIX (incremental largestTimestamp): the running max tracked for the
    // just-sealed segment becomes its entry's timestamp; reset for the new one.
    const auto sealedIt = std::prev(segments_.end(), 2);
    retireSealedReadMap_(sealedIt->first, *sealedIt->second);   // D30
    newlySealedMaxTs_ = activeSegmentMaxTs_;
    newlySealedBase_  = sealedIt->first;
    activeSegmentMaxTs_ = 0;
    // E1d: an inline seal made the sealed segment durable — R may cross into
    // the new active segment (persisted inline on the legacy path only).
    advanceRecoveryPoint_();
    maybeCheckpointInline_();
    // hotpath-A F4: ONE O(1) publish per roll — append the sealed entry.
    snapshotAppendSealed_(sealedIt);
    newlySealedMaxTs_ = 0;
    newlySealedBase_  = -1;
}

// ---------------------------------------------------------------------------
// Sealed-segment snapshot (hotpath-A F4).
// ---------------------------------------------------------------------------
namespace {
// Largest timestamp of a sealed segment's entry: the value it was last
// published with (sealed contents never change under one object), else
// `fresh` (the roll's running max / the recovery scan / 0), never below its
// floor (CRITICAL-1: what the segments it replaced or absorbed published).
inline int64_t sealedEntryTs(LogSegment& seg, int64_t fresh) noexcept {
    int64_t ts = seg.publishedTs() != INT64_MIN ? seg.publishedTs() : fresh;
    if (seg.tsFloor() > ts) ts = seg.tsFloor();
    seg.setPublishedTs(ts);
    return ts;
}
} // namespace

void Log::snapRebaseDir_(std::size_t keepChunks, std::size_t extraChunks) {
    // Cold-ish: once per kSnapHeadersPerDir publishes or when the chunk slots
    // run out — O(live chunks) pointer copies, never an entry copy.
    const std::size_t firstChunk = snapDir_ ? snapBegin_ / kSnapChunkEntries : 0;
    auto dir = std::make_shared<SealedSnapshotDir>();
    dir->chunkCap = std::max<std::size_t>(16, 2 * (keepChunks + extraChunks) + 2);
    dir->chunks.reset(new std::shared_ptr<SealedSnapshotChunk>[dir->chunkCap]);
    for (std::size_t c = 0; c < keepChunks; ++c) dir->chunks[c] = snapDir_->chunks[firstChunk + c];
    dir->headerCap = kSnapHeadersPerDir;
    dir->headers.reset(new SealedSegmentSnapshot[dir->headerCap]);
    snapBegin_ -= firstChunk * kSnapChunkEntries;
    snapEnd_   -= firstChunk * kSnapChunkEntries;
    snapDir_ = std::move(dir);
    snapHeadersUsed_ = 0;
}

void Log::snapPushBack_(const SegInfo& e) {
    const std::size_t c = snapEnd_ / kSnapChunkEntries;
    if (!snapDir_ || c >= snapDir_->chunkCap) {
        const std::size_t live = snapDir_ ? (snapEnd_ + kSnapChunkEntries - 1) / kSnapChunkEntries -
                                                snapBegin_ / kSnapChunkEntries
                                          : 0;
        snapRebaseDir_(live, 1);
    }
    const std::size_t cc = snapEnd_ / kSnapChunkEntries;
    if (snapEnd_ % kSnapChunkEntries == 0 && !snapDir_->chunks[cc]) [[unlikely]]
        snapDir_->chunks[cc] = std::make_shared<SealedSnapshotChunk>();   // once per kSnapChunkEntries
    // Slot snapEnd_ is covered by no published snapshot (see snapEnd_).
    snapDir_->chunks[cc]->e[snapEnd_ % kSnapChunkEntries] = e;
    ++snapEnd_;
}

void Log::publishSnapshotView_() {
    if (!snapDir_ || snapHeadersUsed_ == snapDir_->headerCap) {
        const std::size_t live = snapDir_ ? (snapEnd_ + kSnapChunkEntries - 1) / kSnapChunkEntries -
                                                snapBegin_ / kSnapChunkEntries
                                          : 0;
        snapRebaseDir_(live, 0);
    }
    SealedSegmentSnapshot& h = snapDir_->headers[snapHeadersUsed_++];
    h = SealedSegmentSnapshot(snapDir_.get(), snapBegin_, snapEnd_ - snapBegin_);
    // Aliasing shared_ptr: shares the directory's control block — no allocation.
    ownerSnap_ = std::shared_ptr<const SealedSegmentSnapshot>(snapDir_, &h);
    // D22: hand it to the cleaner through the triple buffer (no lock).
    snapSlot_[snapBack_] = ownerSnap_;
    const uint8_t mine = static_cast<uint8_t>(snapBack_ + kSnapFresh);   // index + fresh bit
    const uint8_t prev = snapMiddle_.exchange(mine, std::memory_order_acq_rel);
    snapBack_ = static_cast<uint8_t>(prev & kSnapIndex);
}

void Log::snapshotAppendSealed_(SegMap::iterator sealed) {
    // The published entries must mirror every sealed segment but the new one
    // (a log driven before its first publish — never recovered — or any other
    // drift): else the tail rebuild, which also covers the new entry.
    const std::size_t live = snapEnd_ - snapBegin_;
    if (segments_.size() < 2 || live != segments_.size() - 2 ||
        (live > 0 && snapAt_(snapEnd_ - 1).endOffset != sealed->first)) [[unlikely]] {
        publishSealedSnapshotFrom_(sealed->first);
        return;
    }
    LogSegment& seg = *sealed->second;
    const int64_t ts = sealedEntryTs(seg, newlySealedMaxTs_);
    snapPushBack_(SegInfo{sealed->first, seg.sizeBytes(), ts, std::next(sealed)->first, seg.tsLineage()});
    publishSnapshotView_();
}

void Log::snapshotDropFront_(std::size_t k) {
    // Retention erases a PREFIX of the sealed segments (N4-LOW-2): the snapshot
    // drops the same prefix — O(1), no entry rewritten.  A mismatch (never
    // expected) falls back to the rebuild.
    const std::size_t live = snapEnd_ - snapBegin_;
    const std::size_t sealedNow = segments_.empty() ? 0 : segments_.size() - 1;
    if (snapDir_ && k <= live && live - k == sealedNow &&
        (sealedNow == 0 || snapAt_(snapBegin_ + k).baseOffset == segments_.begin()->first)) [[likely]] {
        snapBegin_ += k;
        publishSnapshotView_();
        return;
    }
    publishSealedSnapshot();
}

void Log::publishSealedSnapshot() { publishSealedSnapshotFrom_(INT64_MIN); }

void Log::publishSealedSnapshotFrom_(int64_t fromBase) {
    // Timestamps (CP-8 / CP-5.1 / CRITICAL-1): a segment already published
    // keeps its value (sealed contents are immutable under one object — a
    // rebuilt / rewritten segment is a NEW object with publishedTs unset); a
    // never-published one takes the roll's running max or recover()'s
    // one-time scan value (recoveryMaxTimestamps_), else 0 (= unknown); never
    // below its floor.
    auto entryFor = [this](SegMap::iterator it) {
        int64_t scanned = 0;   // MEDIUM-2: 0 is a real value, never "not found"
        if (it->second->publishedTs() == INT64_MIN) {
            if (it->first == newlySealedBase_) {
                scanned = newlySealedMaxTs_;   // the roll's running max (no file I/O)
            } else if (auto rm = recoveryMaxTimestamps_.find(it->first); rm != recoveryMaxTimestamps_.end()) {
                scanned = rm->second;
            }
        }
        const int64_t ts = sealedEntryTs(*it->second, scanned);
        return SegInfo{it->first, it->second->sizeBytes(), ts, std::next(it)->first, it->second->tsLineage()};
    };

    // 1. Where to start: the entry just below the first one at/above fromBase
    //    (binary search over the live entries).  Everything before it is
    //    untouched by the caller's change and stays shared as-is.
    std::size_t start = snapBegin_;
    if (snapDir_ && fromBase != INT64_MIN) {
        std::size_t lo = snapBegin_, hi = snapEnd_;
        while (lo < hi) {
            const std::size_t mid = lo + (hi - lo) / 2;
            if (snapAt_(mid).baseOffset < fromBase) lo = mid + 1; else hi = mid;
        }
        start = lo > snapBegin_ ? lo - 1 : snapBegin_;
    }
    auto it = segments_.begin();
    if (start > snapBegin_) {
        it = segments_.find(snapAt_(start).baseOffset);
        // The kept prefix must end exactly where the map resumes; otherwise
        // (never expected) rebuild everything.
        if (it == segments_.end() || it == segments_.begin() ||
            std::prev(it)->first != snapAt_(start - 1).baseOffset) {
            start = snapBegin_;
            it = segments_.begin();
        }
    }
    const auto activeIt = segments_.empty() ? segments_.end() : std::prev(segments_.end());

    // 2. Walk: keep entries that compare equal; at the first difference copy-
    //    on-write that entry's chunk (its prefix) and write the rest fresh.
    std::size_t i = start;
    bool diverged = false;
    auto divergeAt = [this, &diverged](std::size_t at) {
        diverged = true;
        // New directory: the chunks wholly below `at` stay shared; the chunk
        // holding `at` is copied up to `at` (its later slots may be covered by
        // published snapshots — they are never written again).
        const std::size_t firstChunk = snapDir_ ? snapBegin_ / kSnapChunkEntries : 0;
        const std::size_t atChunk    = at / kSnapChunkEntries;
        std::shared_ptr<SealedSnapshotChunk> partial;
        if (snapDir_ && at % kSnapChunkEntries != 0) {
            partial = std::make_shared<SealedSnapshotChunk>();
            const SealedSnapshotChunk& src = *snapDir_->chunks[atChunk];
            std::copy(src.e, src.e + at % kSnapChunkEntries, partial->e);
        }
        const std::size_t keep = snapDir_ ? atChunk - firstChunk : 0;
        snapEnd_ = atChunk * kSnapChunkEntries;   // everything from atChunk on is re-pushed
        if (!snapDir_) { snapBegin_ = snapEnd_ = 0; }
        snapRebaseDir_(keep, segments_.size() / kSnapChunkEntries + 1);
        if (partial) {
            snapDir_->chunks[snapEnd_ / kSnapChunkEntries] = std::move(partial);
            snapEnd_ += at % kSnapChunkEntries;
        }
    };
    if (!snapDir_) divergeAt(0);
    for (; it != activeIt && it != segments_.end(); ++it) {
        const SegInfo e = entryFor(it);
        if (!diverged && i < snapEnd_) {
            if (snapAt_(i) == e) { ++i; continue; }
            divergeAt(i);
        }
        // (i == snapEnd_ without a divergence: a pure tail append — the slot is
        // covered by no published snapshot, written in place.)
        snapPushBack_(e);
        ++snapRebuiltEntries_;
        ++i;
    }
    if (!diverged && i < snapEnd_) divergeAt(i);   // trailing entries removed (truncation)
    // CP-5.1: the recovery scan is consumed by the first publish after recover().
    recoveryMaxTimestamps_.clear();
    ++snapRebuilds_;
    publishSnapshotView_();
}

bool Log::snapshotMatchesSegmentsForTest() const {
    const auto snap = ownerSealedSnapshot();
    const std::size_t n = segments_.size() > 1 ? segments_.size() - 1 : 0;
    if (!snap || snap->size() != n) return false;
    std::size_t i = 0;
    for (auto it = segments_.begin(); i < n; ++it, ++i) {
        const SegInfo& si = (*snap)[i];
        if (si.baseOffset != it->first || si.sizeBytes != it->second->sizeBytes() ||
            si.endOffset != std::next(it)->first || si.lineage != it->second->tsLineage() ||
            si.largestTimestamp != it->second->publishedTs())
            return false;
    }
    return true;
}
// ---------------------------------------------------------------------------
// Ready pool (hotpath-A F1 + F2).
// ---------------------------------------------------------------------------
int Log::provisionReady(std::size_t depth, bool preallocate) {
    // PROVISIONING thread only (the ring's single producer).  NEVER touches
    // segments_.  Every expensive step of a roll happens here: create + open
    // the files, ftruncate + mmap + prefault the index, map the read view, and
    // fallocate the segment's blocks (F1).
    if (depth == 0) depth = 1;
    if (depth > kMaxProvisionDepth) depth = kMaxProvisionDepth;
    int built = 0;
    // At most `depth` builds per call: the owner may consume as fast as this
    // thread builds, and an unbounded loop would starve the caller's other
    // duties for this log (finalizeStagingRenames between refills).
    for (std::size_t b = 0; b < depth; ++b) {
        if (readyQueuedApprox() >= depth) break;
        const int64_t seq = static_cast<int64_t>(stagingSeq_++);
        std::unique_ptr<LogSegment> seg;
        int err = 0;
        try {
            if (provisionFaultN_ > 0) [[unlikely]] {
                --provisionFaultN_;
                throw std::system_error(provisionFaultErr_, std::generic_category(), "provision fault (test)");
            }
            seg = LogSegment::prepareStaging(stagingLogPathFor(seq), stagingIndexPathFor(seq), seq,
                                             kDefaultIndexIntervalBytes, indexMaxBytes_);
        } catch (const std::system_error& e) {
            err = e.code().value() != 0 ? e.code().value() : EIO;
        } catch (...) {
            err = EIO;
        }
        if (err != 0) {
            std::error_code ec;   // a half-built staging file holds no data
            std::filesystem::remove(stagingLogPathFor(seq), ec);
            std::filesystem::remove(stagingIndexPathFor(seq), ec);
            // Review M1: never silent — the owner learns it over the ring and
            // rolls inline (a failing inline create then fails the log).
            reportProvisionFailure(err);
            return -err;
        }
        // F1: reserve the blocks HERE (was fallocate(rollBytes) on the owner at
        // every roll — 150-330 µs).  KEEP_SIZE: O_APPEND still writes at offset 0.
        // Re-review N3: over the node-wide reservation cap the segment is still
        // built off the owner — only the block reservation is shed.
        if (preallocate) seg->preallocate(rollBytes_);
        // D19 L2: the map node the owner will install this segment into, built
        // here (an allocation the owner no longer makes per roll).  Best effort:
        // without one the owner falls back to its spare pool / an emplace.
        try {
            SegMap tmp;
            tmp.emplace(int64_t{0}, nullptr);
            seg->setSpareMapNode(tmp.extract(tmp.begin()));
        } catch (...) {}
        LogSegment* raw = seg.release();
        // Publish: the ring's commit hands the fully built segment (files, maps,
        // blocks) to the owner's poll in pullReady_().
        if (!readyRing_.offer(kReadyMsg, static_cast<int64_t>(reinterpret_cast<std::uintptr_t>(raw)), 0)) {
            const int64_t s2 = raw->stagingSeq();
            delete raw;   // ring full (failure records queued): nothing lost, retried on demand
            std::error_code ec;
            std::filesystem::remove(stagingLogPathFor(s2), ec);
            std::filesystem::remove(stagingIndexPathFor(s2), ec);
            // D19 N6d: never silent — counted, logged (rate-limited) and
            // reported as a failed build.  The queued records each end an
            // owner hold, so no demand is left without an answer.
            // Sanitizer triage 2026-10-14: `(++readyDrops_ & (readyDrops_ - 1))` was an
            // unsequenced modify+read (UB, -Wunsequenced) — the first drop could go unlogged.
            const uint64_t drops = ++readyDrops_;
            if ((drops & (drops - 1)) == 0)
                spdlog::warn("Log {}: ready ring full — a built segment was discarded (drops={})", dir_, drops);
            return built > 0 ? built : -ENOBUFS;
        }
        ++built;
    }
    return built;
}

void Log::reportProvisionFailure(int err) noexcept {
    // Provisioning thread.  A full ring already holds records the owner will
    // pull first (each ends a hold), so a dropped failure record loses nothing.
    if (readyRing_.offer(kFailedMsg, err != 0 ? err : EIO, 0)) {
        // D19 N6b: remember where it ends (this thread is the one producer).
        failMarkEnd_[failMarkNext_++ % kFailMarks] = readyRing_.producerPosition();
    }
}

void Log::pullReady_() noexcept {
    // OWNER (the ring's one consumer).  One record per call into the front slot.
    if (readyFront_ != nullptr || failFront_ != 0) return;
    (void)readyRing_.poll([this](int32_t type, int64_t a, int64_t) noexcept {
        if (type == kReadyMsg) {
            readyFront_ = reinterpret_cast<LogSegment*>(static_cast<std::uintptr_t>(a));
        } else if (type == kFailedMsg) {
            failFront_ = a != 0 ? static_cast<int>(a) : EIO;
            ++provisionFailures_;
            // Counted; logged rate-limited (powers of two — no timer).
            if ((provisionFailures_ & (provisionFailures_ - 1)) == 0)
                spdlog::warn("Log {}: segment provisioning failed (errno={}, failures={}): the next roll "
                             "creates its segment inline", dir_, failFront_, provisionFailures_);
        }
    }, 1);
}

void Log::postProvisionDemands_(int64_t activeBytes) noexcept {
    // OWNER.  Cold: at most a few times per segment (a failed offer is retried
    // at the next append / hold check — a demand is never lost).
    if (provisionChannel_ == nullptr) return;
    if (adoptedPostPending_ && postDemand_(ProvisionDemand::Adopted))
        adoptedPostPending_ = false;
    if (!thresholdPosted_ && activeBytes >= provisionThresholdBytes_ && postDemand_(ProvisionDemand::Threshold))
        thresholdPosted_ = true;
}

LogSegment* Log::takeReady_() noexcept {
    pullReady_();
    LogSegment* seg = readyFront_;
    readyFront_ = nullptr;
    return seg;
}

void Log::installNextSegment(int64_t base) {
    // OWNER thread only.
    // A provisioned log adopts only when its rename can be queued (a FORCED
    // roll past the overshoot cap with a full ring creates inline instead).
    // Review M1: a pulled provisioning FAILURE makes this roll inline (below).
    pullReady_();
    // D19 N6b: a failure followed by a ready segment is stale (a later build
    // succeeded) — skip it and adopt; only a failure with nothing behind it
    // forces the inline roll.
    bool failureRoll = false;
    while (provisioned_ && readyFront_ == nullptr && failFront_ != 0) {
        failFront_ = 0;
        failureRoll = true;
        pullReady_();
    }
    if (readyFront_ != nullptr) failureRoll = false;
    if (failureRoll) ++failureInlineRolls_;
    if (LogSegment* prepared = (!provisioned_ || renameRoom_()) && !failureRoll ? takeReady_() : nullptr) {
        // ADOPT — ZERO syscalls: pure in-memory rebase.  The staging .log/.index
        // keep their temp names on disk for now; the disk rename (staging →
        // <base>.{log,index}) is deferred to the background thread.
        std::unique_ptr<LogSegment> seg(prepared);
        // D19 L2: keep the node the provisioner built (only while the spare
        // pool has room — the pool never reallocates; an untaken node is
        // freed with its segment, off the roll).
        if (spareSegNodes_.size() < spareSegNodes_.capacity())
            if (SegMap::node_type nh = seg->takeSpareMapNode(); !nh.empty()) spareSegNodes_.push_back(std::move(nh));
        const int64_t stagingSeq = seg->stagingSeq();
        seg->adoptAsActive(base);            // in-memory only
        install_(base, std::move(seg));
        // The deferred rename (SPSC; rolls are rare so the ring never fills in
        // practice).  A full ring: rename synchronously (legacy logs only).
        // A provisioned log never gets here with a full ring: maybeRoll defers
        // its roll until the ring has room (rollBlockedNow_ / renameRoom_).
        static_assert(kRenameRingCapacity > kMaxProvisionDepth);
        if (!enqueueStagingRename(base, stagingSeq) && !provisioned_) {
            renameStagingToBase(stagingSeq, base);
            auto it = segments_.find(base);
            if (it != segments_.end())
                it->second->setLogPath(logPathForBase(base));
        }
        // F2: event-driven — the provisioner finalizes the rename (Adopted).
        adoptedPostPending_ = true;
    } else {
        // LEGACY (non-provisioned) logs only — maybeRoll defers a provisioned
        // log's roll instead.  Correctness fallback: synchronous create (+ the
        // F1 preallocation, inline here by necessity).  F3 fix-round-1: built
        // into a LOCAL and installed only once construction fully succeeded — a
        // failed roll leaves segments_ untouched (the just-sealed segment stays
        // active), the exception propagates, the next append retries.
        auto seg = std::make_unique<LogSegment>(
            dir_, base, kDefaultIndexIntervalBytes, indexMaxBytes_,
            /*prefaultIndex*/false, /*eagerReadMap*/true, /*readMapBytes*/0, fileMode_);
        seg->preallocate(rollBytes_);
        ++ownerPreallocs_;
        install_(base, std::move(seg));
    }
    // MEDIUM-2: a new directory entry (the provisioner created the adopted
    // staging file before publishing it; the fallback created it just now).
    // A crash before its rename leaves the staging name, which construction
    // reconciles — so the entry is durable once a directory fsync issued
    // after this point completes.
    segments_.rbegin()->second->setDirEntryGen(++dirMutGen_);
    // Lows A/B/C review M3: mapReaders_ decays — a log with no readView since
    // the previous roll (a finished rebuild walk) stops provisioning read maps
    // for its new active segments (each an off-owner mmap nobody reads).  The
    // next readView sets it again.
    if (readViewCalls_ == readViewsAtRoll_) mapReaders_ = false;
    readViewsAtRoll_ = readViewCalls_;
    // An outstanding growth request was for the segment just sealed (its
    // answer is discarded as superseded — seq-matched, re-review N2): the new
    // active segment may request its own mapping at once.
    growOutstanding_ = false;
    growWaits_ = 0;
    // Review M3: the new active segment demands its successor at the threshold.
    thresholdPosted_ = false;
    holdChecks_ = 0;            // a hold episode ends with the roll
    holdPostPending_ = false;
    if (provisioned_) postProvisionDemands_(0);
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
    if (!renameRoom_()) return false;   // D19 N6c: bounded with the done ring
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
    std::size_t i = head;
    for (; i != tail; ++i) {
        // hotpath-A: a request is consumed only when its completion can be
        // published — a dropped completion would leave the owner's logPath_ at
        // the staging name for good (and a cold path re-opening it by that
        // path would fail the log).  Unreachable since D19 N6c: the owner
        // enqueues only while requests + undrained completions < capacity
        // (renameRoom_), so the done ring always has room here; kept as a guard.
        const std::size_t dt = renameDoneTail_.load(std::memory_order_relaxed);
        const std::size_t dh = renameDoneHead_.load(std::memory_order_acquire);
        if (dt - dh >= kRenameRingCapacity) break;
        const auto rr = renameReqRing_[i & kRenameRingMask];
        renameStagingToBase(rr.seq, rr.base);
        renameDoneRing_[dt & kRenameRingMask] = rr.base;
        renameDoneTail_.store(dt + 1, std::memory_order_release);
        ++count;
    }
    if (count > 0) renameReqHead_.store(i, std::memory_order_release);
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
        if (it != segments_.end()) it->second->setCanonicalLogPath(dir_, base);   // no allocation
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
    // D19 N5a: the canonical segment bases — a recovered base must fit the
    // chain between them (an inline roll can create a canonical successor
    // while an adopted segment's rename is still queued).
    std::vector<int64_t> canonical;
    for (auto& e : std::filesystem::directory_iterator(dir_, ec)) {
        const std::string fn = e.path().filename().string();
        if (fn.rfind(kStagingPrefix, 0) != 0) {
            if (e.path().extension() == ".log") {
                const std::string stem = e.path().stem().string();
                int64_t b = -1;
                const auto [ptr, perr] = std::from_chars(stem.data(), stem.data() + stem.size(), b);
                if (perr == std::errc() && ptr == stem.data() + stem.size()) canonical.push_back(b);
            }
            continue;
        }
        if (e.path().extension() == ".stlog")      stagingLogs.push_back(e.path());
        else if (e.path().extension() == ".stidx") stagingIdx.push_back(e.path());
    }
    // Read one whole batch at `pos` (nullopt-style: empty = absent / torn / CRC-invalid).
    auto readBatch = [](std::ifstream& f, int64_t pos, int64_t fileSize) -> std::vector<uint8_t> {
        constexpr int64_t kOv = rune::protocol::batch::kRecordBatchOverhead;
        if (fileSize - pos < kOv) return {};
        std::vector<uint8_t> hdr(static_cast<std::size_t>(kOv));
        f.clear();
        f.seekg(pos);
        if (!f.read(reinterpret_cast<char*>(hdr.data()), kOv)) return {};
        const int32_t total = rune::protocol::RecordBatchReader(hdr.data(), hdr.size()).totalLength();
        if (total < kOv || total > fileSize - pos) return {};
        std::vector<uint8_t> b(static_cast<std::size_t>(total));
        f.seekg(pos);
        if (!f.read(reinterpret_cast<char*>(b.data()), total)) return {};
        if (!rune::protocol::RecordBatchReader(b.data(), b.size()).ensureValid()) return {};
        return b;
    };
    // D19 N5b: the last offset of a file's valid batch prefix (-1 = none) —
    // logged for both candidates of a duplicate base (crash recovery only).
    auto lastOffsetOf = [&](const std::filesystem::path& p) -> int64_t {
        std::ifstream g(p, std::ios::binary);
        if (!g.is_open()) return -1;
        g.seekg(0, std::ios::end);
        const int64_t size = g.tellg();
        int64_t pos = 0, last = -1;
        for (;;) {
            const auto b = readBatch(g, pos, size);
            if (b.empty()) break;
            rune::protocol::RecordBatchReader br(b.data(), b.size());
            last = br.baseOffset() + br.lastOffsetDelta();
            pos += static_cast<int64_t>(b.size());
        }
        return last;
    };
    struct Recovered { std::filesystem::path path; int64_t base; };
    std::vector<Recovered> found;
    auto quarantine = [&](const std::filesystem::path& lp, const std::string& why) {
        // Kept (renamed .stlog.corrupt), never parsed into the log and never
        // reclaimed automatically — the operator decides (D19 N5b).
        spdlog::error("Log {}: staging file {} {} — quarantined as .corrupt (never parsed / overwritten)",
                      dir_, lp.string(), why);
        std::filesystem::rename(lp, lp.string() + ".corrupt", ec);
    };
    for (const auto& lp : stagingLogs) {
        std::ifstream f(lp, std::ios::binary);
        int64_t fileSize = 0;
        if (f.is_open()) { f.seekg(0, std::ios::end); fileSize = f.tellg(); f.seekg(0, std::ios::beg); }
        if (fileSize < static_cast<int64_t>(rune::protocol::batch::kRecordBatchOverhead)) {
            f.close();
            std::filesystem::remove(lp, ec); // empty / torn — never adopted
            continue;
        }
        // Review m4: trust the base only from a COMPLETE, CRC-valid first
        // batch, and never overwrite an existing segment.  Anything else is
        // quarantined; recovery's chain validation handles the resulting gap
        // like any lost write.
        const auto first = readBatch(f, 0, fileSize);
        if (first.empty()) { f.close(); quarantine(lp, "has no valid first batch"); continue; }
        rune::protocol::RecordBatchReader r(first.data(), first.size());
        const int64_t base = r.baseOffset();
        const int64_t end1 = base + int64_t{r.lastOffsetDelta()} + 1;
        // D19 N5a: baseOffset is not under the CRC — cross-check it.  A
        // CRC-valid second batch must start exactly where the first ends.
        const auto second = readBatch(f, static_cast<int64_t>(first.size()), fileSize);
        f.close();
        if (!second.empty() && rune::protocol::RecordBatchReader(second.data(), second.size()).baseOffset() != end1) {
            quarantine(lp, "first batch base " + std::to_string(base) + " does not chain to its next batch");
            continue;
        }
        if (std::find(canonical.begin(), canonical.end(), base) != canonical.end()) {
            {
                // D19 N5b: both candidates' extents, so an operator can decide.
                quarantine(lp, "would overwrite existing segment " + logPathForBase(base) + " (base " +
                                   std::to_string(base) + "; staging last offset " + std::to_string(lastOffsetOf(lp)) +
                                   ", existing last offset " + std::to_string(lastOffsetOf(logPathForBase(base))) + ")");
            }
            continue;
        }
        found.push_back({lp, base});
    }
    // D19 N5a: in base order, each recovered base must lie at or above the
    // end of the segment below it and its records below the segment above it
    // (a corrupted base cannot overlay live offsets).  Compaction gaps are fine.
    std::sort(canonical.begin(), canonical.end());
    std::sort(found.begin(), found.end(), [](const Recovered& x, const Recovered& y) { return x.base < y.base; });
    for (const auto& rec : found) {
        auto above = std::upper_bound(canonical.begin(), canonical.end(), rec.base);
        if (above != canonical.begin()) {
            const int64_t prevBase = *std::prev(above);
            const int64_t prevEnd  = std::max(prevBase, lastOffsetOf(logPathForBase(prevBase)) + 1);
            if (rec.base < prevEnd) {
                quarantine(rec.path, "base " + std::to_string(rec.base) + " overlaps segment " +
                                         std::to_string(prevBase) + " (ends at " + std::to_string(prevEnd) + ")");
                continue;
            }
        }
        if (above != canonical.end() && lastOffsetOf(rec.path) >= *above) {
            quarantine(rec.path, "records of base " + std::to_string(rec.base) + " overlap segment " +
                                     std::to_string(*above));
            continue;
        }
        std::filesystem::rename(rec.path, logPathForBase(rec.base), ec);
        if (!ec) canonical.insert(std::upper_bound(canonical.begin(), canonical.end(), rec.base), rec.base);
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
LogReadView Log::readInto(int64_t startOffset, int32_t maxBytes, uint8_t* buf, std::size_t bufCap,
                          bool& firstTooLarge) noexcept {
    ReadIntoStatus st;
    const LogReadView v = readInto(startOffset, maxBytes, buf, bufCap, st);
    firstTooLarge = st == ReadIntoStatus::FirstTooLarge;
    return v;
}
LogReadView Log::readInto(int64_t startOffset, int32_t maxBytes, uint8_t* buf, std::size_t bufCap,
                          ReadIntoStatus& st, int64_t boundOffset, int* errOut) noexcept {
    RUNE_FR_SPAN_READ(rune_fr::Site::ReadLogRead, 0);
    st = ReadIntoStatus::Absent;
    auto it = segments_.upper_bound(startOffset);
    if (it == segments_.begin()) return {};
    --it;
    LogReadView v = it->second->readInto(startOffset, maxBytes, buf, bufCap, st, boundOffset, errOut);
    // D26: nothing at/after startOffset in its segment — a compacted TAIL gap
    // of a sealed segment (or an emptied one): continue at the next segment's
    // base (below the bound), else a cursor there is answered empty forever.
    // Never past torn / unreadable bytes or an I/O error.  One compare when found.
    while (st == ReadIntoStatus::Absent && std::next(it) != segments_.end()) [[unlikely]] {
        ++it;
        if (it->first >= boundOffset) break;
        v = it->second->readInto(std::max(startOffset, it->first), maxBytes, buf, bufCap, st, boundOffset, errOut);
    }
    return v;
}
LogReadView Log::readView(int64_t startOffset, int32_t maxBytes) noexcept {
    RUNE_FR_SPAN_READ(rune_fr::Site::ReadLogRead, 0);
    ++readViewCalls_;
    mapReaders_ = true;   // D26: growth is provisioned only for logs read through the mapping
    auto it = segments_.upper_bound(startOffset);
    if (it == segments_.begin()) return {};
    --it;
    if (asyncReadGrowth_) {
        // Review Md5: the mapping grows OFF the owner.
        pollReadMaps_();
        LogSegment& seg = *it->second;
        const bool active = std::next(it) == segments_.end();
        maybeGrowReadMap_(seg, it->first, active);
        // Review M3: the provisioner has not answered the active segment's
        // map request yet — the owner maps it now (counted, cold).
        if (active && seg.readMapBytes() == 0 && seg.sizeBytes() > 0) [[unlikely]] ++ownerFallbackRemaps_;
        // Re-review N3: never stall readers on the provisioner — data already
        // past the mapping is served by a (counted, cold) owner remap.
        const bool beyond = seg.readMapBytes() != 0 && static_cast<std::size_t>(seg.sizeBytes()) > seg.readMapBytes();
        if (beyond) [[unlikely]] ++ownerFallbackRemaps_;
        seg.setOwnerRemap(beyond);
    }
    const bool sealedUnmapped = std::next(it) != segments_.end() && it->second->readMapBytes() == 0;
    const LogReadView v = it->second->readView(startOffset, maxBytes);
    if (sealedUnmapped && it->second->readMapBytes() != 0) [[unlikely]] noteSealedMapped_(it->first);
    return v;
}

void Log::maybeGrowReadMap_(LogSegment& seg, int64_t base, bool active) noexcept {
    const std::size_t mapped = seg.readMapBytes();
    // Lows A/B/C review M3: an UNMAPPED active segment of a log with a mapped
    // reader (D26: staged segments carry no read map) is mapped off the owner
    // too — requested from 0 at the first append after the roll — so the next
    // readView finds it mapped instead of mmapping the whole active segment on
    // the owner.  A sealed one is mapped lazily by readView (D30).
    if (mapped == 0 && (!active || !mapReaders_)) return;
    // D21 re-review-2 low: a SEALED segment never grows off the owner — it
    // wants growth only when read beyond its mapping, and readView remaps it
    // on the owner in that same call (re-review N3), so an async answer could
    // only be discarded (a dup + an off-owner mmap for nothing).
    if (!active) return;
    const auto size = static_cast<std::size_t>(seg.sizeBytes());
    // Active: grow at half full, to the full-segment size.
    const bool want = size * 2 > mapped;
    if (!want) return;
    if (growOutstanding_) {
        // Count-based re-request (a request lost to a full ring / a racing view
        // change): never time-based, never wedged.
        if (size > mapped && ++growWaits_ >= 4096) { growOutstanding_ = false; growWaits_ = 0; }
        if (growOutstanding_) return;
    }
    std::size_t target = std::max(LogSegment::readMapBytesFor(rollBytes_), size * 2);
    target = (target + (std::size_t{1} << 20) - 1) >> 20;          // MiB
    if ((target << 20) <= mapped) return;
    const int fd = ::fcntl(seg.logFd(), F_DUPFD_CLOEXEC, 0);       // the provisioner owns the dup
    if (fd < 0) return;
    const int64_t seq = (growSeq_ + 1) & 0x7FFFF;
    if (!provisionChannel_->post(ProvisionChannel::kGrowMsgType,
                                 static_cast<int64_t>(reinterpret_cast<std::uintptr_t>(this)),
                                 packRequest(fd, static_cast<int64_t>(target), seq))) {
        ::close(fd);
        return;
    }
    growSeq_ = seq;
    ++readMapGrowRequests_;
    growOutstanding_ = true;
    growBase_ = base;
    growGen_ = seg.installGen();
    growWaits_ = 0;
}

void Log::retireSealedReadMap_(int64_t base, LogSegment& seg) noexcept {
    // D30: a sealed segment holds no read mapping — its bytes are read with
    // pread (#65 / D26), and a later readView remaps lazily (F3).  Two mappings
    // per segment (index + read map) exhausted a default vm.max_map_count
    // (65530) at ~32k segments.  An answer to a growth request for it is
    // orphaned (discarded + disposed on arrival).  The munmap runs off the
    // owner (the provisioner's dispose), inline only without a channel / with
    // the ring full.
    if (growOutstanding_ && growBase_ == base) growBase_ = INT64_MIN;
    std::unique_ptr<MappedFile> m = seg.swapReadMap(nullptr);
    if (!m) return;
    ++readMapsRetiredAtSeal_;
    disposeReadMap_(std::move(m));
}

void Log::disposeReadMap_(std::unique_ptr<MappedFile> m) noexcept {
    // Off the owner (the provisioner munmaps) when it serves read maps; inline
    // (counted: a munmap = mmap_lock write on this thread) without one, with a
    // test intercept (it never disposes — review MEDIUM-1: posting there leaked
    // the mapping), or with the ring full.
    if (asyncReadGrowth_ &&
        provisionChannel_->post(ProvisionChannel::kDisposeMsgType,
                                static_cast<int64_t>(reinterpret_cast<std::uintptr_t>(m.get())), 0)) {
        (void)m.release();
        return;
    }
    ++readMapsDisposedInline_;
}

void Log::releaseLazySealedMaps() noexcept {
    // D30 re-review M1: a finished walk of old data (an RC build after a restart
    // / leadership move) releases the sealed maps it brought back — an idle
    // partition holds none (thousands of partitions x the cap would be a real
    // share of vm.max_map_count).
    for (std::size_t i = 0; i < mappedSealedCount_; ++i) {
        const int64_t b = mappedSealed_[(mappedSealedHead_ + i) % kMappedSealedCap];
        const auto it = segments_.find(b);
        if (it == segments_.end() || std::next(it) == segments_.end()) continue;
        if (auto m = it->second->swapReadMap(nullptr)) {
            ++sealedMapsEvicted_;
            disposeReadMap_(std::move(m));
        }
    }
    mappedSealedHead_ = mappedSealedCount_ = 0;
}

void Log::noteSealedMapped_(int64_t base) noexcept {
    // D30 review MAJOR-1: a sealed segment mapped again (a readView / RC walk of
    // old data) stays mapped — without a bound, one walk of a long log brings
    // back a mapping per segment (vm.max_map_count).  At most kMappedSealedCap
    // lazily mapped sealed segments; the oldest retires.  Safe: a LogReadView is
    // valid only until the next read of this log, and the eviction runs inside
    // that next read.
    ++sealedLazyRemaps_;
    if (mappedSealedCount_ == kMappedSealedCap) {
        const int64_t victim = mappedSealed_[mappedSealedHead_];
        mappedSealedHead_ = (mappedSealedHead_ + 1) % kMappedSealedCap;
        --mappedSealedCount_;
        const auto vit = segments_.find(victim);
        if (vit != segments_.end() && victim != base && std::next(vit) != segments_.end()) {
            if (auto m = vit->second->swapReadMap(nullptr)) {
                ++sealedMapsEvicted_;
                disposeReadMap_(std::move(m));
            }
        }
    }
    mappedSealed_[(mappedSealedHead_ + mappedSealedCount_) % kMappedSealedCap] = base;
    ++mappedSealedCount_;
}

void Log::pollReadMaps_() noexcept {
    (void)mapRing_.poll([this](int32_t type, int64_t a, int64_t b) {
        if (type == kMapFailedMsg) {
            if ((a & 0x7FFFF) != growSeq_) return;   // re-review N2: a superseded request
            // Counted; this log falls back to growing on the owner (correct, slow).
            ++readMapGrowthFailures_;
            growOutstanding_ = false;
            asyncReadGrowth_ = false;
            for (auto& [base, s] : segments_) if (s) s->setOwnerRemap(true);
            return;
        }
        if (type != kMapGrownMsg) return;
        std::unique_ptr<MappedFile> m(reinterpret_cast<MappedFile*>(static_cast<std::uintptr_t>(a)));
        // Re-review N2: only the answer to the OUTSTANDING request (its seq) is
        // installed, and only into the segment it was made for (base + install
        // generation); a late answer to a superseded request maps another file.
        const bool current = b == growSeq_ && growOutstanding_;
        if (current) growOutstanding_ = false;
        auto it = current ? segments_.find(growBase_) : segments_.end();
        if (!current) ++readMapsDiscarded_;
        if (it != segments_.end() && it->second && it->second->installGen() == growGen_ &&
            m->size() > it->second->readMapBytes()) {
            m = it->second->swapReadMap(std::move(m));   // m = the retired mapping
            ++readMapGrowths_;
        }
        // Retire off the owner (munmap); inline only if the ring is full.
        if (m && provisionChannel_->post(ProvisionChannel::kDisposeMsgType,
                                         static_cast<int64_t>(reinterpret_cast<std::uintptr_t>(m.get())), 0))
            (void)m.release();
    }, 8);
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
    startupRp_ = corrupt ? logStartOffset() : rp;
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
    // A directory holding a replica's epoch history guards itself (a passthrough
    // log never has one, so nothing would ever lift the guard there).
    if (!epochGuard_) {
        std::error_code e1, e2;
        epochGuard_ = std::filesystem::exists(dir_ + "/leader-epoch-state", e1) ||
                      std::filesystem::exists(dir_ + "/leader-epoch-checkpoint", e2);
    }
    if (epochGuard_ && !corrupt) {
        // Review C1: hold R at the startup value until the epoch history the
        // owner re-derives from it is durable (setEpochHistoryCap lifts it).
        const int64_t hold = std::max<int64_t>(logStartOffset(), std::min<int64_t>(startupRp_, next));
        rpGuardTarget_ = next;
        rpEpochCap_ = hold;
        recoveryPoint_ = hold;
    }
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
        // Re-review N5: created with the HELD value (the epoch guard), never past it.
        if (rf.state == RpFileState::Missing) (void)createRecoveryPointFile_(recoveryPoint_);
        else if (storage_.failedErrno == 0)  (void)checkpointRecoveryPoint();
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
    // log on the first doWork() call after a restart (ownerSealedSnapshot()
    // returns nullptr → candidate selection skips this replica) and the
    // persisted checkpoint is never loaded via ensureCheckpoint().
    // CP-5.1: recoveryMaxTimestamps_ is consumed (and cleared) by this call.
    publishSealedSnapshot();
    // D28: recovery reopened the active segment's index un-prefaulted; map it
    // writable here (startup, before the owner runs) so the owner's appends
    // into its tail never fault on mmap_lock.  Sealed indexes are read-only
    // from here on (and rebuildEntries already touched their entries).
    activeSegment().prefaultIndex();
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
    if (err == 0 && storage_.writeFaultForTest > 0) err = storage_.writeFaultForTest;
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
    setPersistedRp_(r);
    ++rpPersists_;
    return 0;
}

int Log::checkpointRecoveryPoint() noexcept {
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
    if (storage_.writeFaultForTest > 0) err = storage_.writeFaultForTest;
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
    // hotpath-A F4: the cleaner gate moves with ONE release-store (no snapshot
    // republish — the snapshot no longer carries the recovery point).
    setPersistedRp_(v);
    rpUnconfirmedMax_ = kRecoveryPointUnknown;   // every possibly-landed write has a LOWER seq
    rpActiveSlot_ = target;
    ++rpPersists_;
    refreshRpDue_();
    return 0;
}

void Log::refreshRpDue_() noexcept {
    // Hot-path B F7: an armed truncation needs the on-disk value lowered.
    rpLowerDue_ = rpTruncCap_ != INT64_MAX && onDiskRecoveryPointUpperBound() > rpTruncCap_;
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
    if (r > rpTruncCap_) r = rpTruncCap_;   // hot-path B F7: never above an armed truncation's cut
    if (r > rpEpochCap_) r = rpEpochCap_;   // review C1: never past epoch history not yet durable
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

void Log::setEpochHistoryCap(int64_t c) noexcept {
    const bool raised = c > rpEpochCap_;
    rpEpochCap_ = c;
    if (recoveryPoint_ > c) [[unlikely]] recoveryPoint_ = c;   // in memory only (callers never cap below R)
    if (raised && rpGuardTarget_ >= 0) {
        // The recovery-time R the guard held back (everything recovered is durable).
        const int64_t t = std::min(rpGuardTarget_, c);
        if (t > recoveryPoint_) recoveryPoint_ = t;
        if (c >= rpGuardTarget_) rpGuardTarget_ = -1;
        refreshRpDue_();
        if (recoveryPoint_ > persistedRp_) rpDue_ = true;
    }
    if (raised) {
        advanceRecoveryPoint_();
        if (recoveryCheckpointDue() || directorySyncDue()) rpKick_ = true;
    }
}

bool Log::prepareTruncationTo(int64_t x) noexcept {
    if (!recovered_ || !deferSealSync_ || rpFd_ < 0 || storage_.failedErrno != 0) return true;   // inline path
    if (x >= logEndOffset_) return true;                          // nothing to truncate
    // The resulting LEO: truncation rounds down to the start of the batch
    // holding x (truncateTo's own rule) — lower R exactly there, so truncateTo's
    // trailing persistAtMost_(LEO) writes nothing either.
    int64_t cut = std::max<int64_t>(0, x);
    const LogReadView v = readView(cut, 0);
    if (!v.empty() && v.baseOffset < cut) cut = v.baseOffset;
    if (cut < rpTruncCap_) rpTruncCap_ = cut;
    if (recoveryPoint_ > rpTruncCap_) recoveryPoint_ = rpTruncCap_;
    const bool staleInProgress = rp_.stage != RpStage::Idle && !rp_.abandoned && rp_.value > rpTruncCap_;
    refreshRpDue_();
    if (!staleInProgress && onDiskRecoveryPointUpperBound() <= rpTruncCap_) return true;
    if (recoveryCheckpointDue()) { rpKick_ = true; ++rpLowerings_; }
    return false;
}

bool Log::prepareRecoveryCheckpoint(RpWrite& out) noexcept {
    if (!recoveryCheckpointDue()) return false;
    const int slot = rpActiveSlot_ < 0 ? 0 : 1 - rpActiveSlot_;
    const uint64_t seq = ++rpSeq_;
    encodeRpSlot(rpImage_, seq, recoveryPoint_);
    rp_ = RpCheckpoint{RpStage::Prepared, false, slot, seq, recoveryPoint_};
    rpDue_ = false;
    rpLowerDue_ = false;
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
        // hotpath-A F4: the cleaner gate moves with ONE release-store — no second
        // snapshot publish per roll (was an O(segments) rebuild + allocation).
        setPersistedRp_(done.value);
        rpUnconfirmedMax_ = kRecoveryPointUnknown;   // the highest-seq write: confirmed
        rpActiveSlot_ = done.slot;
        ++rpPersists_;
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
    } else if (!spareSegNodes_.empty()) {
        // hotpath-A: reuse a released node (no allocation on the roll path).
        SegMap::node_type nh = std::move(spareSegNodes_.back());
        spareSegNodes_.pop_back();
        nh.key()    = base;
        nh.mapped() = std::move(seg);
        segments_.insert(std::move(nh));
    } else {
        segments_.emplace(base, std::move(seg));
    }
}

Log::SegMap::iterator Log::eraseKeepNode_(SegMap::iterator it) noexcept {
    auto next = std::next(it);
    SegMap::node_type nh = segments_.extract(it);
    // Only an EMPTY node is kept (its segment was moved out / handed off); the
    // vector never grows past its reservation (no allocation here either).
    if (!nh.mapped() && spareSegNodes_.size() < spareSegNodes_.capacity()) spareSegNodes_.push_back(std::move(nh));
    return next;
}

Log::SegMap::iterator Log::erase_(SegMap::iterator it) {
    retire_(std::move(it->second));
    return eraseKeepNode_(it);
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
    // Review Md5: the RC scan needs the WHOLE segment (a partial view would read
    // as "walked") — it may map on the owner (cold: RC builds; counted remaps).
    it->second->setOwnerRemap(true);
    const bool sealedUnmapped = std::next(it) != segments_.end() && it->second->readMapBytes() == 0;
    const LogReadView v = it->second->mappedBytes();
    if (sealedUnmapped && it->second->readMapBytes() != 0) [[unlikely]] noteSealedMapped_(it->first);
    if (asyncReadGrowth_) it->second->setOwnerRemap(false);
    b.data = v.data;
    b.size = v.len;
    b.written = static_cast<std::size_t>(std::max<int64_t>(it->second->sizeBytes(), 0));
    return b;
}
std::vector<int64_t> Log::segmentBaseOffsets() const {
    std::vector<int64_t> v; for (auto& [b, _] : segments_) v.push_back(b); return v;
}
int64_t Log::totalBytes() const {
    int64_t t = 0; for (auto& [b, seg] : segments_) t += seg->sizeBytes(); return t;
}
bool Log::truncateTo(int64_t truncateOffset) {
    const bool done = truncateTo_(truncateOffset);
    if (done) clearTruncationCap_();   // hot-path B F7: R may advance again
    return done;
}

bool Log::truncateTo_(int64_t truncateOffset) {
    if (truncateOffset >= logEndOffset_) return true; // nothing to do
    // Fix round 5: never touch a file by path while the cleaner still has
    // posted renames to perform — deferred, no wait (the caller retries).
    {
        // Touched: the segment holding truncateOffset and every later one.
        auto t = segments_.upper_bound(truncateOffset);
        const int64_t touchFrom = t == segments_.begin() ? INT64_MIN : std::prev(t)->first;
        if (!beginColdPath_(touchFrom)) return false;
    }
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
                                                 /*eagerReadMap*/true, /*readMapBytes*/0, fileMode_);
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
    // Hot-path B review M2: a worker-driven log truncates it IN PLACE (one
    // ftruncate of the open fd, index entries zeroed in its mapping): no new
    // file, no index rename — the directory changes only if whole segments
    // were deleted above, so only then is a directory fsync owed.
    if (deferSealSync_) {
        LogSegment& aseg = activeSegment();
        int64_t keptMaxTs = 0;
        const int err = aseg.truncateInPlace(truncateOffset, keptMaxTs);
        if (err > 0) [[unlikely]] {
            spdlog::critical("Log {}: in-place truncation to {} failed (errno={}) — the cut could not be "
                             "placed at the requested offset; log FAILED (fail-stop)", dir_, truncateOffset, err);
            markStorageFailed(err);   // nothing changed in the segment; fail-stop (the owner halts)
            return true;
        }
        if (err == 0) {
            if (deletesFiles) aseg.setDirEntryGen(++dirMutGen_);   // MEDIUM-2: the unlinks above
            logEndOffset_ = aseg.nextOffset();
            activeSegmentMaxTs_ = keptMaxTs;
            ++inPlaceTruncations_;
            noteTruncatedTo_(logEndOffset_);
            persistAtMost_(logEndOffset_);
            publishSealedSnapshotFrom_(aseg.baseOffset());
            return true;
        }
        // err < 0: no read mapping — rebuild the segment below.
    }
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
        if (total < rune::protocol::batch::kRecordBatchOverhead || pos + static_cast<std::size_t>(total) > buf.size()) break;
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
                                                    /*eagerReadMap*/true, /*readMapBytes*/0, fileMode_);
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
        // Hot-path B F7: a worker-driven log never msyncs / directory-fsyncs
        // on its owner here — the rebuilt index is then as durable as a
        // deferred seal leaves it (page cache), and its rename is a directory
        // mutation R may not pass until the owner's batched async directory
        // fsync covered it (MEDIUM-2, below).
        activeSegment().recover(/*syncIndex=*/!deferSealSync_);
        activeSegment().setDurableNext(std::min(oldDurable, activeSegment().nextOffset()));
        // MEDIUM-2: R stays below the deleted files' range until a directory
        // fsync covered the deletions (else they could reappear trusted).
        if (deletesFiles || deferSealSync_) activeSegment().setDirEntryGen(++dirMutGen_);   // + the index rename
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
    // cleaner's activeBase / sealed set match.  hotpath-A F4: only the tail from
    // the (new) active segment is rebuilt — the sealed prefix below it is shared.
    publishSealedSnapshotFrom_(activeSegment().baseOffset());
    return true;
}

bool Log::reloadSegments() {
    if (!beginColdPath_(INT64_MIN)) return false;   // fix round 5: posted renames first (deferred, no wait)
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
                                                       /*eagerReadMap*/ b == bases.back(),
                                                       /*readMapBytes*/0, fileMode_);
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
    // hotpath-A F4: only the tail from this segment is rebuilt.
    publishSealedSnapshotFrom_(baseOffset);
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
    install_(baseOffset, std::make_unique<LogSegment>(dir_, baseOffset, kDefaultIndexIntervalBytes, indexMaxBytes_,
                                                 false, false, 0, fileMode_));
    LogSegment& fresh = *segments_[baseOffset];
    fresh.raiseTsFloor(floorTs);
    fresh.recover();
    // E1d: the cleaner fsyncs a rewritten segment before its rename.
    fresh.setDurableNext(wasDurable ? fresh.nextOffset() : std::min(oldDurable, fresh.nextOffset()));
}

int Log::deleteOldSegments(int64_t retainBytes, int64_t highWatermark) {
    // Review m6: queued renames never block it (it stops at a pathPending segment).
    if (!beginColdPath_(INT64_MAX)) return 0;   // fix round 5: posted renames first (retried next pass)
    int deleted = 0;
    int64_t total = totalBytes();   // review LOW-2: one O(n) sum, then a running total
    while (segments_.size() > 1 && total > retainBytes) {
        auto it = segments_.begin();
        // MAJOR-3 (Kafka: HW >= nextSegment.baseOffset): never delete a segment
        // holding records above the HW (not yet replicated / committed).
        if (std::next(it)->first > highWatermark) break;
        if (it->second->pathPending()) break;   // its staging rename is in flight (review m6)
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
    if (deleted > 0) snapshotDropFront_(static_cast<std::size_t>(deleted));   // hotpath-A F4: a prefix
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
            m = eraseKeepNode_(m);
        }
    } else {
        // Every record removed: drop all members (their offsets read as a gap,
        // as with Kafka compaction).  Their timestamps fold into the predecessor.
        if (it != segments_.begin()) std::prev(it)->second->raiseTsFloor(membersMaxTs);
        auto m = it;
        for (std::size_t k = 0; k < n; ++k) {
            msgs[1 + k].kind = MsgKind::Dispose; msgs[1 + k].base = m->first; msgs[1 + k].obj = m->second.release();
            m = eraseKeepNode_(m);
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
    if (durable && newStart > persistedLogStart_) { ++cleanerFsyncs_; ++cleanerDirFsyncs_; }   // gate attribution
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
        ++cleanerDirFsyncs_;
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

bool Log::beginColdPath_(int64_t touchFrom) noexcept {
    // hotpath-A: never touch a segment by path while its staging -> <base>
    // rename is in flight (the provisioner renames promptly; the in-memory
    // logPath_ only follows once the owner drains the done ring).  A QUEUED
    // rename defers the cold path only if its segment is one the cold path
    // touches (base >= touchFrom — review m6).  The caller retries — no wait.
    //
    // Re-review N1 — ORDER MATTERS: acquire the request head FIRST, then drain
    // the done ring.  The provisioner publishes a completion (done ring) before
    // it releases the request slot, so every rename below the acquired head is
    // visible to the drain that follows; a rename that completes after the
    // acquire is still inside [head, tail) and defers.  (Draining first missed
    // a rename completing in between: the cold path then deleted the segment by
    // its stale staging path and the real <base>.log survived.)
    const std::size_t reqTail = renameReqTail_.load(std::memory_order_relaxed);   // owner = producer
    const std::size_t reqHead = renameReqHead_.load(std::memory_order_acquire);
    if (coldSeamFn_ != nullptr) [[unlikely]] coldSeamFn_(coldSeamCtx_);
    (void)drainStagingRenamesDone();
    // Slots in [head, tail) were written by this (producer) thread and are only
    // read by the consumer: reading them here races nothing.
    for (std::size_t i = reqHead; i != reqTail; ++i)
        if (renameReqRing_[i & kRenameRingMask].base >= touchFrom) return false;
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
    int64_t lowestBase = INT64_MAX;   // hotpath-A F4: the snapshot is rebuilt from here
    for (std::size_t i = head; i != tail; ) {
        const SegRef ref = swapRing_[i & kSwapRingMask];
        if (ref.base < lowestBase) lowestBase = ref.base;
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
        // so the cleaner thread sees correct sizes on the next scan — hotpath-A
        // F4: only the tail from the lowest affected base is rebuilt.
        publishSealedSnapshotFrom_(lowestBase);
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
        (void)eraseKeepNode_(it);
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
    // Refresh the sealed snapshot so the cleaner sees the updated segment list:
    // hotpath-A F4 — the erased segments are a prefix, so the snapshot only
    // advances its front (O(1), no rebuild).
    if (count > 0) snapshotDropFront_(static_cast<std::size_t>(count));
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
