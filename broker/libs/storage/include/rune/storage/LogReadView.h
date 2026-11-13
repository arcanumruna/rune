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
    /// #874: the read reached unservable bytes at its start offset (empty; the consumer fails, it must not wait).
    bool           damaged    = false;
    bool empty() const noexcept { return data == nullptr || len == 0; }
};

/// #65 / D26: the outcome of a pread-served read (LogSegment / Log::readInto).
enum class ReadIntoStatus : uint8_t {
    Ok,             ///< the view holds the first batch at/after the start offset
    Absent,         ///< no batch at/after the start offset in the bytes scanned (segment end / compacted tail gap)
    FirstTooLarge,  ///< the first deliverable batch does not fit the caller's buffer (caller may fall back)
    Unreadable,     ///< torn / corrupt bytes at the cursor, or no fd: empty, never skipped past
    IoError,        ///< pread failed (or the file is shorter than its written size): empty — NEVER
                    ///< retried through the read mapping (that would SIGBUS on the same bad page).
                    ///< The log is then storage-failed (sticky, review M1): every later readInto
                    ///< answers IoError without a pread; the owner routes it to the halt.
    Corrupt,        ///< the read reached the damage of a damaged sealed segment (bytes kept on disk, never
                    ///< served); never skipped past (#839).  Log::readInto's scan stops on it.
};

/// A status a consumer must fail loudly on rather than wait out: damaged bytes (Corrupt) or torn length fields at the cursor
/// (Unreadable).  Both leave the view empty, and nothing at that cursor will ever become readable.
[[nodiscard]] constexpr bool unservable(ReadIntoStatus s) noexcept {
    return s == ReadIntoStatus::Corrupt || s == ReadIntoStatus::Unreadable;
}

} // namespace rune::storage
