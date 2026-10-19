#include <rune/storage/OffsetIndex.h>
#include <climits>
#include <cstring>
#include <stdexcept>
namespace rune::storage {
OffsetIndex::OffsetIndex(std::string path, int64_t baseOffset, int32_t maxBytes, bool prefault, unsigned fileMode)
    : baseOffset_(baseOffset) {
    file_ = std::make_unique<MappedFile>(std::move(path), maxBytes, true, prefault, fileMode);
}
void OffsetIndex::append(int64_t offset, int64_t position) {
    // C4: Guard the 2GiB position limit here (where the cast happens) so all callers
    // — LogSegment, LogCleaner::buildIndex — are uniformly protected.
    if (position > static_cast<int64_t>(INT32_MAX))
        throw std::runtime_error("rune: OffsetIndex position exceeds 2GiB limit");
    int32_t rel = static_cast<int32_t>(offset - baseOffset_);
    if (rel < 0) throw std::runtime_error("offset below base");
    int32_t pos32 = static_cast<int32_t>(position);
    // Grow-on-demand: the index mapping is now RIGHT-SIZED per segment (small
    // initial mmap instead of a fixed 8 MiB over-provision).  If a segment ever
    // produces more entries than the initial estimate (e.g. an oversized segment
    // or an unusually dense index interval), double the mapping so the write
    // never runs past the end.  In steady state this never fires — the initial
    // size (Log::indexMaxBytesFor) already covers a full rollBytes segment plus
    // generous headroom.  Growth is off the append fast path (only on overflow).
    const size_t needBytes = (static_cast<size_t>(entries_) + 1) *
                             static_cast<size_t>(kEntrySize);
    if (needBytes > file_->size()) {
        size_t newSize = file_->size();
        if (newSize == 0) newSize = static_cast<size_t>(kEntrySize);
        while (newSize < needBytes) newSize *= 2;
        file_->resize(newSize);
    }
    uint8_t* p = file_->data() + entries_ * static_cast<size_t>(kEntrySize);
    std::memcpy(p, &rel, 4);
    std::memcpy(p + 4, &pos32, 4);
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
int64_t OffsetIndex::truncateToPosition(int64_t pos) noexcept {
    uint8_t* p = const_cast<uint8_t*>(file_->data());
    while (entries_ > 0) {
        int32_t epos = 0;
        std::memcpy(&epos, p + static_cast<size_t>(entries_ - 1) * static_cast<size_t>(kEntrySize) + 4, 4);
        if (epos < pos) break;
        std::memset(p + static_cast<size_t>(entries_ - 1) * static_cast<size_t>(kEntrySize), 0,
                    static_cast<size_t>(kEntrySize));
        --entries_;
    }
    lastSearchLow_ = 0;
    if (entries_ == 0) return -1;
    int32_t last = 0;
    std::memcpy(&last, p + static_cast<size_t>(entries_ - 1) * static_cast<size_t>(kEntrySize) + 4, 4);
    return last;
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
