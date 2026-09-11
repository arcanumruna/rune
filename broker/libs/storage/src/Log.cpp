#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <filesystem>
#include <algorithm>
#include <fstream>
namespace rune::storage {
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
    if (activeSegment().sizeBytes() >= rollBytes_) {
        int64_t base = logEndOffset_;
        segments_[base] = std::make_unique<LogSegment>(dir_, base);
    }
}
int64_t Log::append(const std::vector<uint8_t>& batch, int64_t lastOffset) {
    maybeRoll();
    logEndOffset_ = activeSegment().append(batch, lastOffset);
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
        std::string ipath = lpath.substr(0, lpath.size() - 4) + ".index";
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

    // Walk batches to find the byte offset where baseOffset >= truncateOffset.
    std::size_t cutPos = buf.size(); // default: keep everything (shouldn't happen)
    std::size_t pos = 0;
    while (pos + static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead) <= buf.size()) {
        rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
        int32_t total = r.totalLength();
        if (total <= 0 || pos + static_cast<std::size_t>(total) > buf.size()) break;
        if (r.baseOffset() >= truncateOffset) {
            cutPos = pos;
            break;
        }
        pos += static_cast<std::size_t>(total);
        cutPos = pos; // valid end so far
    }

    // Truncate the file at cutPos.
    std::filesystem::resize_file(lpath, static_cast<std::uintmax_t>(cutPos));
    // Also truncate the index — simplest: just let recover() rebuild it.
    // Re-open the segment via recover() to get correct state.
    std::string ipath = lpath.substr(0, lpath.size() - 4) + ".index";
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

int Log::deleteOldSegments(int64_t retainBytes) {
    int deleted = 0;
    while (segments_.size() > 1 && totalBytes() > retainBytes) {
        auto it = segments_.begin();
        std::string base = it->second->logPath();
        std::string idx = base.substr(0, base.size()-4) + ".index";
        segments_.erase(it);
        std::filesystem::remove(base);
        std::filesystem::remove(idx);
        ++deleted;
    }
    return deleted;
}
} // namespace rune::storage
