#pragma once
#include <rune/storage/MappedFile.h>
#include <string>
#include <memory>
namespace rune::storage {
struct OffsetPosition { int64_t offset; int32_t position; };
class OffsetIndex {
public:
    /// @param maxBytes  initial size of the mmap-backed index file.  This is now
    ///   RIGHT-SIZED per segment (see Log::indexMaxBytesFor) rather than a fixed
    ///   8 MiB over-provision; append() grows the mapping on demand if a segment
    ///   ever needs more entries than the initial estimate (rare — only oversized
    ///   segments), so a small initial size is always safe.
    /// @param prefault  touch every page of the fresh mapping so the cold
    ///   minor-page-faults are paid HERE (ideally on a background thread) rather
    ///   than on the append/roll hot path at the first index write.
    OffsetIndex(std::string path, int64_t baseOffset, int32_t maxBytes, bool prefault = false, unsigned fileMode = 0644);
    void append(int64_t offset, int64_t position);
    OffsetPosition lookup(int64_t targetOffset) const;
    // CP-10: msync only the written prefix (entries_ * kEntrySize) instead of the
    // full over-provisioned 8 MiB mapping.  The untouched tail is zero-filled and
    // never dirtied, so syncing it is pure waste on every seal / durable append.
    [[nodiscard]] int sync() { return file_->sync(static_cast<size_t>(entries_) * static_cast<size_t>(kEntrySize)); }
    /// Rescan the mmap to count valid entries written by a previous process.
    /// Call after opening an existing index file (e.g. after recover() renames
    /// .index.tmp → .index and reopens from the canonical path).
    void rebuildEntries();
    int entries() const noexcept { return entries_; }
    /// D28: map every page writable now (off the owner's append path).
    void prefault() noexcept { file_->prefaultWrite(); }
    [[nodiscard]] const uint8_t* dataForTest() const noexcept { return file_->data(); }
    [[nodiscard]] size_t mappedBytesForTest() const noexcept { return file_->size(); }
    int64_t baseOffset() const noexcept { return baseOffset_; }
    /// Rebase an EMPTY index (used when adopting a staging segment: the index was
    /// built at a provisional base 0 with zero entries; adopting the segment at
    /// its real base just updates the relative-offset origin).  Only valid while
    /// entries_ == 0.
    void setBaseOffset(int64_t base) noexcept { baseOffset_ = base; }
    int lastSearchLow() const { return lastSearchLow_; }
    /// Hot-path B review M2: drop (zero) every entry at or past byte `pos` —
    /// an in-place tail truncation of the segment.  Memory only (no syscall).
    /// Returns the position of the last kept entry (-1 = none).
    int64_t truncateToPosition(int64_t pos) noexcept;
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
