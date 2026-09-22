#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
namespace rune::storage {

namespace {
// CL4: Shared helper to derive the .index path from a .log path.
// Avoids fragile repeated ".substr(0, path.size()-4) + ".index"" surgery.
inline std::string indexPathForLog(const std::string& logPath) {
    return logPath.substr(0, logPath.size() - 4) + ".index";
}
} // anonymous namespace

Log::Log(std::string dir, int64_t segmentRollBytes) : dir_(std::move(dir)), rollBytes_(segmentRollBytes) {
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
    if (bases.empty()) bases.push_back(0);
    for (int64_t b : bases) segments_[b] = std::make_unique<LogSegment>(dir_, b);
    logEndOffset_ = activeSegment().nextOffset();
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
        activeSegment().sync();
        int64_t base = logEndOffset_;
        segments_[base] = std::make_unique<LogSegment>(dir_, base);
        // CP-8 FIX (incremental largestTimestamp): capture the running max we
        // tracked for the about-to-be-sealed segment, then reset for the new
        // active segment.  publishSealedSnapshot will use this captured value
        // (via newlySealedMaxTs_) instead of re-scanning the file.
        newlySealedMaxTs_ = activeSegmentMaxTs_;
        activeSegmentMaxTs_ = 0;
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
    // a newly-sealed segment.  Instead, maybeRoll() maintains activeSegmentMaxTs_
    // (a running max of maxTimestamp() across every batch appended to the active
    // segment) and stores it in newlySealedMaxTs_ just before calling us.
    // For segments already present in the previous snapshot, we reuse the
    // cached largestTimestamp value (sealed segments are immutable).
    // For the newly-sealed segment (not yet in prevSnap), we use newlySealedMaxTs_.
    // Zero extra I/O on the append path: NO file open, NO read, NO heap allocation.
    auto prevSnap = std::atomic_load(&sealedSnapshot_);

    auto snap = std::make_shared<SealedSegmentSnapshot>();
    if (segments_.size() > 1) {
        snap->reserve(segments_.size() - 1);
        auto it  = segments_.begin();
        auto end = std::prev(segments_.end()); // stop before active
        for (; it != end; ++it) {
            int64_t base  = it->first;
            int64_t bytes = it->second->sizeBytes();
            // Try to reuse largestTimestamp from the previous snapshot for this
            // segment (sealed segments are immutable after they are created).
            int64_t largestTs = 0;
            if (prevSnap) {
                for (const auto& si : *prevSnap) {
                    if (si.baseOffset == base) { largestTs = si.largestTimestamp; break; }
                }
            }
            // If not found in the previous snapshot, this is the NEWLY SEALED
            // segment.  Use the incrementally tracked max timestamp instead of
            // re-scanning the file (CP-8 fix: zero append-path I/O at seal time).
            if (largestTs == 0) {
                largestTs = newlySealedMaxTs_;
            }
            snap->push_back({base, bytes, largestTs});
        }
    }
    std::atomic_store(&sealedSnapshot_, std::shared_ptr<const SealedSegmentSnapshot>(std::move(snap)));
}
int64_t Log::append(const uint8_t* data, std::size_t len, int64_t lastOffset) {
    // Zero-alloc hot path: LogSegment::append(pointer) writes directly from the
    // caller's buffer — no std::vector construction on the produce path.
    // CP-8: call maybeRoll() FIRST (which may seal the old segment and reset
    // activeSegmentMaxTs_), THEN update activeSegmentMaxTs_ for the NEW batch
    // being written to the (possibly freshly-started) active segment.
    // This ensures the incremental counter tracks batches that land on the
    // current active segment, NOT the batch that caused the roll.
    maybeRoll();
    logEndOffset_ = activeSegment().append(data, len, lastOffset);
    // Update incremental largestTimestamp AFTER the write (and after any roll)
    // so this batch's maxTimestamp is attributed to the segment it was written to.
    if (len >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
        rune::protocol::RecordBatchReader r(data, len);
        int64_t batchMaxTs = r.maxTimestamp();
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
    maybeRoll();
    logEndOffset_ = activeSegment().appendNoSync(data, len, lastOffset);
    if (len >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
        rune::protocol::RecordBatchReader r(data, len);
        int64_t batchMaxTs = r.maxTimestamp();
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
int64_t Log::recover() {
    int64_t next = 0;
    for (auto& [base, seg] : segments_) next = seg->recover();
    logEndOffset_ = next;
    // Publish sealed snapshot so the LogCleanerAgent can see pre-existing
    // sealed segments immediately after recovery (without waiting for the first
    // new append to trigger maybeRoll()).  Without this, the cleaner skips the
    // log on the first doWork() call after a restart (sealedSegmentSnapshot()
    // returns nullptr → candidate selection skips this replica) and the
    // persisted checkpoint is never loaded via ensureCheckpoint().
    publishSealedSnapshot();
    return next;
}
void Log::sync() { activeSegment().sync(); }
std::vector<int64_t> Log::segmentBaseOffsets() const {
    std::vector<int64_t> v; for (auto& [b, _] : segments_) v.push_back(b); return v;
}
int64_t Log::totalBytes() const {
    int64_t t = 0; for (auto& [b, seg] : segments_) t += seg->sizeBytes(); return t;
}
void Log::truncateTo(int64_t truncateOffset) {
    if (truncateOffset >= logEndOffset_) return; // nothing to do

    // Remove segments that start at or beyond truncateOffset.
    auto it = segments_.lower_bound(truncateOffset);
    while (it != segments_.end()) {
        std::string lpath = it->second->logPath();
        std::string ipath = indexPathForLog(lpath);
        std::filesystem::remove(lpath);
        std::filesystem::remove(ipath);
        it = segments_.erase(it);
    }

    if (segments_.empty()) {
        // Re-create a fresh segment at truncateOffset (or 0 if negative).
        int64_t base = std::max<int64_t>(0, truncateOffset);
        segments_[base] = std::make_unique<LogSegment>(dir_, base);
        logEndOffset_ = base;
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
    // Also truncate the index — simplest: just let recover() rebuild it.
    // Re-open the segment via recover() to get correct state.
    std::string ipath = indexPathForLog(lpath);
    std::filesystem::remove(ipath);

    // Rebuild the active segment from disk.
    int64_t base = seg.baseOffset();
    segments_.erase(base);
    segments_[base] = std::make_unique<LogSegment>(dir_, base);
    // Re-run recover to rebuild the index and set nextOffset correctly.
    // Use the segment's actual nextOffset_ (derived from the physically truncated
    // file) rather than hard-coding truncateOffset, so that LEO and the segment's
    // nextOffset_ can never diverge.
    activeSegment().recover();
    logEndOffset_ = activeSegment().nextOffset();
}

void Log::reloadSegments() {
    // Destroy all in-memory segments (closes open file handles).
    segments_.clear();
    // Re-discover segment files on disk (same logic as the constructor).
    std::vector<int64_t> bases;
    for (auto& e : std::filesystem::directory_iterator(dir_))
        if (e.path().extension() == ".log")
            bases.push_back(std::stoll(e.path().stem().string()));
    std::sort(bases.begin(), bases.end());
    if (bases.empty()) bases.push_back(0);
    for (int64_t b : bases) segments_[b] = std::make_unique<LogSegment>(dir_, b);
    // Run recover() to rebuild indices and set nextOffset_ for each segment.
    int64_t next = 0;
    for (auto& [base, seg] : segments_) next = seg->recover();
    logEndOffset_ = next;
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

    const std::string logPath = it->second->logPath();
    if (!std::filesystem::exists(logPath)) {
        // Cleaner emptied + deleted this segment; drop it from the map.
        segments_.erase(it);
        return;
    }

    // Reconstruct just this segment (closes the old fd via unique_ptr reset) and
    // recover() it so its in-memory index reflects the rewritten bytes.
    it->second = std::make_unique<LogSegment>(dir_, baseOffset);
    it->second->recover();
}

int Log::deleteOldSegments(int64_t retainBytes) {
    int deleted = 0;
    while (segments_.size() > 1 && totalBytes() > retainBytes) {
        auto it = segments_.begin();
        std::string base = it->second->logPath();
        std::string idx = indexPathForLog(base);
        segments_.erase(it);
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
            segments_.erase(it);
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
