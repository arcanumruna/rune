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
    void sync() { file_->sync(); }  // msync the mmap to disk
    /// Rescan the mmap to count valid entries written by a previous process.
    /// Call after opening an existing index file (e.g. after recover() renames
    /// .index.tmp → .index and reopens from the canonical path).
    void rebuildEntries();
    int entries() const noexcept { return entries_; }
    int64_t baseOffset() const noexcept { return baseOffset_; }
    int lastSearchLow() const { return lastSearchLow_; }
protected:
    int slotFor(int64_t target) const;         // largest-lower-bound slot, -1 if none
    OffsetPosition parseEntry(int slot) const;
    static constexpr int kEntrySize = 8;
    static constexpr int kWarmEntries = 8192 / kEntrySize;
    int64_t baseOffset_; int entries_ = 0;
    mutable int lastSearchLow_ = 0;
    std::unique_ptr<MappedFile> file_;
};
} // namespace rune::storage
