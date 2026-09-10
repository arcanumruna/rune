#pragma once
#include <rune/storage/MappedFile.h>
#include <string>
#include <memory>
namespace rune::storage {
struct OffsetPosition { int64_t offset; int32_t position; };
class OffsetIndex {
public:
    OffsetIndex(std::string path, int64_t baseOffset, int32_t maxBytes);
    void append(int64_t offset, int32_t position);
    OffsetPosition lookup(int64_t targetOffset) const;
    int entries() const noexcept { return entries_; }
    int64_t baseOffset() const noexcept { return baseOffset_; }
protected:
    int slotFor(int64_t target) const;         // largest-lower-bound slot, -1 if none
    OffsetPosition parseEntry(int slot) const;
    static constexpr int kEntrySize = 8;
    int64_t baseOffset_; int entries_ = 0;
    std::unique_ptr<MappedFile> file_;
};
} // namespace rune::storage
