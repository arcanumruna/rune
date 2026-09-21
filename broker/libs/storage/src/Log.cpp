#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <algorithm>
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
    for (auto& e : std::filesystem::directory_iterator(dir_))
        if (e.path().extension() == ".log")
            bases.push_back(std::stoll(e.path().stem().string()));
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
        // Publish snapshot: a new sealed segment just appeared.
        publishSealedSnapshot();
    }
}

void Log::publishSealedSnapshot() {
    // Build a fresh immutable snapshot of all sealed segments (all except the
    // last/active).  Called on the single-writer append thread only.
    // The active segment is excluded: its sizeBytes is mutated on appends.
    //
    // CP-8: also carry largestTimestamp per sealed segment so the cleaner thread
    // can gate on minCompactionLagMs without touching segments_.  We derive it
    // from the existing snapshot if available (avoid re-scanning unchanged segs).
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
            // If not found in previous snapshot (newly sealed), scan the file.
            if (largestTs == 0) {
                std::string logPath = it->second->logPath();
                std::ifstream f(logPath, std::ios::binary);
                if (f.is_open()) {
                    f.seekg(0, std::ios::end);
                    auto sz = static_cast<std::size_t>(f.tellg());
                    if (sz > 0) {
                        std::vector<uint8_t> buf(sz);
                        f.seekg(0);
                        f.read(reinterpret_cast<char*>(buf.data()),
                               static_cast<std::streamsize>(sz));
                        std::size_t pos = 0;
                        while (pos + static_cast<std::size_t>(
                                   rune::protocol::batch::kRecordBatchOverhead) <= sz) {
                            rune::protocol::RecordBatchReader r(buf.data() + pos, sz - pos);
                            int32_t total = r.totalLength();
                            if (total <= 0 ||
                                pos + static_cast<std::size_t>(total) > sz) break;
                            // Use maxTimestamp (the actual record timestamps,
                            // not the horizon-overloaded baseTimestamp).
                            int64_t batchMax = r.maxTimestamp();
                            if (batchMax > largestTs) largestTs = batchMax;
                            pos += static_cast<std::size_t>(total);
                        }
                    }
                }
            }
            snap->push_back({base, bytes, largestTs});
        }
    }
    std::atomic_store(&sealedSnapshot_, std::shared_ptr<const SealedSegmentSnapshot>(std::move(snap)));
}
int64_t Log::append(const uint8_t* data, std::size_t len, int64_t lastOffset) {
    // Zero-alloc hot path: LogSegment::append(pointer) writes directly from the
    // caller's buffer — no std::vector construction on the produce path.
    maybeRoll();
    logEndOffset_ = activeSegment().append(data, len, lastOffset);
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
    maybeRoll();
    logEndOffset_ = activeSegment().appendNoSync(data, len, lastOffset);
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

} // namespace rune::storage
