#include <rune/storage/Log.h>
#include <filesystem>
#include <algorithm>
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
