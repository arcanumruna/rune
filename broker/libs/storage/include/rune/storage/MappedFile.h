#pragma once
#include <cstdint>
#include <cstddef>
#include <string>
namespace rune::storage {
class MappedFile {
public:
    /// @param prefault  when true, touch one byte per page after mapping so the
    ///   cold minor-page-faults are paid HERE (ideally on a background thread)
    ///   rather than lazily on the caller's first write.  Only meaningful for a
    ///   writable mapping.  Used by the segment pre-provisioner so the append/
    ///   roll hot path never faults index pages.
    MappedFile(std::string path, size_t initialBytes, bool writable, bool prefault = false);
    ~MappedFile();
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
    uint8_t* data() noexcept { return data_; }
    const uint8_t* data() const noexcept { return data_; }
    size_t size() const noexcept { return size_; }
    void resize(size_t newBytes);
    /// msync the ENTIRE mapping (MS_SYNC).
    void sync();
    /// TEST-ONLY: the POSIX fd currently held for this mapping.  For a read-only
    /// mapping (writable == false) this is -1 once the mapping is established
    /// (F3: the fd is closed right after mmap — dead weight for a read-only
    /// mapping that is never ftruncate'd).  Writable mappings keep their fd open
    /// for the object's lifetime (needed for ftruncate on resize()).
    [[nodiscard]] int fdForTest() const noexcept { return fd_; }
    /// CP-10: msync only the first `bytes` of the mapping (page-aligned up),
    /// clamped to size_.  The OffsetIndex over-provisions an 8 MiB mapping but
    /// writes only a few KiB of real entries; syncing the full 8 MiB on every
    /// seal / durable append walks 2048 mostly-zero pages for nothing.  Syncing
    /// just the written prefix is correctness-equivalent (the untouched tail is
    /// zero-filled and never dirtied) and materially cheaper.
    void sync(size_t bytes);
private:
    std::string path_; uint8_t* data_ = nullptr; size_t size_ = 0; int fd_ = -1; bool writable_;
    void mapNow();
};
} // namespace rune::storage
