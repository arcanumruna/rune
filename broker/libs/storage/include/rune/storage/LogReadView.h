#pragma once
#include <cstddef>
#include <cstdint>

namespace rune::storage {

/// Zero-copy view over WHOLE record batches (disk: a segment's read mmap;
/// memory: the InMemoryLog ring).  Valid until the next append/truncate on the
/// owning log's thread — consume synchronously.  (Disk: a later readView() on the
/// same segment may remap-on-grow, so never hold a view across another readView.)
struct LogReadView {
    const uint8_t* data       = nullptr;
    std::size_t    len        = 0;
    int64_t        baseOffset = 0;
    int64_t        nextOffset = 0;
    bool empty() const noexcept { return data == nullptr || len == 0; }
};

} // namespace rune::storage
