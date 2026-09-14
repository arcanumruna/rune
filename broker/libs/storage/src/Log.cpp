#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <filesystem>
#include <algorithm>
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
        int64_t base = logEndOffset_;
        segments_[base] = std::make_unique<LogSegment>(dir_, base);
    }
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
} // namespace rune::storage
