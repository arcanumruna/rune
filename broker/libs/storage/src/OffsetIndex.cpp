#include <rune/storage/OffsetIndex.h>
#include <cstring>
#include <stdexcept>
namespace rune::storage {
OffsetIndex::OffsetIndex(std::string path, int64_t baseOffset, int32_t maxBytes)
    : baseOffset_(baseOffset) {
    file_ = std::make_unique<MappedFile>(std::move(path), maxBytes, true);
}
void OffsetIndex::append(int64_t offset, int32_t position) {
    int32_t rel = static_cast<int32_t>(offset - baseOffset_);
    if (rel < 0) throw std::runtime_error("offset below base");
    uint8_t* p = file_->data() + entries_ * kEntrySize;
    std::memcpy(p, &rel, 4);
    std::memcpy(p + 4, &position, 4);
    ++entries_;
}
OffsetPosition OffsetIndex::parseEntry(int slot) const {
    const uint8_t* p = file_->data() + slot * kEntrySize;
    int32_t rel, pos; std::memcpy(&rel, p, 4); std::memcpy(&pos, p + 4, 4);
    return { baseOffset_ + rel, pos };
}
int OffsetIndex::slotFor(int64_t target) const {
    if (entries_ == 0) return -1;
    int firstWarm = entries_ - 1 - kWarmEntries; if (firstWarm < 0) firstWarm = 0;
    int begin, end;
    if (parseEntry(firstWarm).offset < target) { begin = firstWarm; end = entries_ - 1; }
    else {
        if (parseEntry(0).offset > target) { lastSearchLow_ = 0; return -1; }
        begin = 0; end = firstWarm;
    }
    lastSearchLow_ = begin;
    int lo = begin, hi = end;
    while (lo < hi) { int mid = (lo + hi + 1) >> 1;
        if (parseEntry(mid).offset <= target) lo = mid; else hi = mid - 1; }
    return lo;
}
OffsetPosition OffsetIndex::lookup(int64_t target) const {
    int slot = slotFor(target);
    if (slot < 0) return { baseOffset_, 0 };
    return parseEntry(slot);
}
void OffsetIndex::rebuildEntries() {
    entries_ = 0;
    const uint8_t* p = file_->data();
    size_t cap = file_->size() / static_cast<size_t>(kEntrySize);
    for (size_t i = 0; i < cap; ++i) {
        int32_t rel = 0, pos = 0;
        std::memcpy(&rel, p + i * static_cast<size_t>(kEntrySize), 4);
        std::memcpy(&pos, p + i * static_cast<size_t>(kEntrySize) + 4, 4);
        // The mmap is zero-filled beyond written entries.
        // The first entry may legitimately have rel==0 and pos==0 (baseOffset, position 0).
        // Subsequent entries with both fields == 0 mark the end of written data.
        if (i > 0 && rel == 0 && pos == 0) break;
        ++entries_;
    }
}
} // namespace rune::storage
