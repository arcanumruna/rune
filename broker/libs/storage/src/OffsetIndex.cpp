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
    int lo = 0, hi = entries_ - 1;
    if (parseEntry(0).offset > target) return -1;
    while (lo < hi) {
        int mid = (lo + hi + 1) >> 1;
        if (parseEntry(mid).offset <= target) lo = mid; else hi = mid - 1;
    }
    return lo;
}
OffsetPosition OffsetIndex::lookup(int64_t target) const {
    int slot = slotFor(target);
    if (slot < 0) return { baseOffset_, 0 };
    return parseEntry(slot);
}
} // namespace rune::storage
