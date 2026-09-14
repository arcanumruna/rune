#pragma once
#include <rune/storage/OffsetIndex.h>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <fstream>
#include <unistd.h>
namespace rune::storage {

/// A single on-disk log segment: a ".log" data file paired with a ".index" mmap.
///
/// Durability policy:
///   append() calls sync() after every write (durable-by-default).
///   sync() flushes the libc fstream buffer via flush(), then fdatasyncs the
///   ".log" POSIX fd so bytes reach storage, then msyncs the ".index" mmap so
///   the index and log are consistent on disk after a crash.
///
///   This adds per-append latency. A future group-commit path can batch several
///   appends and call sync() once — but callers MUST NOT advance the
///   high-watermark past an offset until sync() has returned for that offset.
///
///   appendNoSync() writes to page cache without calling sync().  This is the
///   gateway group-commit path: the caller MUST eventually call sync() before
///   acking any offset as fsync-durable.  acks=Leader/acks=1 semantics: a
///   leader crash before the next sync() may lose un-flushed acked records.
class LogSegment {
public:
    LogSegment(std::string dir, int64_t baseOffset, int32_t indexIntervalBytes = 4096);
    ~LogSegment();
    /// Raw-pointer overload — avoids heap allocation on the produce hot path.
    /// [data, data+len) must be a valid record batch.  Returns nextOffset.
    /// Calls sync() after writing (durable-by-default).
    int64_t append(const uint8_t* data, std::size_t len, int64_t lastOffset);
    /// Vector overload (kept for API compatibility; delegates to the pointer overload).
    int64_t append(const std::vector<uint8_t>& batch, int64_t lastOffset); // returns nextOffset

    /// No-sync append — writes to page cache only; does NOT call sync().
    /// The gateway group-commit path uses this for every per-produce write and
    /// then calls sync() once per duty-cycle (one fsync amortised over all
    /// produces in that cycle = group-commit).
    ///
    /// Durability contract (acks=Leader / acks=1):
    ///   - Written to the OS page cache immediately → visible to Log::read().
    ///   - NOT fsync-durable: a leader crash before the next sync() call may
    ///     lose the most-recent un-flushed acked records.
    ///   - True crash-durability requires acks=All + ISR replication (Milestone C).
    ///
    /// Callers MUST NOT advance the durable high-watermark (ISR HWM) past an
    /// offset until sync() has returned for that offset.
    int64_t appendNoSync(const uint8_t* data, std::size_t len, int64_t lastOffset);

    /// Flush libc buffer + fdatasync ".log" + msync ".index".
    /// Called automatically by append() (durable-by-default).
    /// External callers (e.g. group-commit) may call this explicitly after
    /// batching multiple appendNoSync() calls.
    void sync();

    std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes);
    int64_t recover();

    int64_t baseOffset() const noexcept { return baseOffset_; }
    int64_t sizeBytes() const noexcept { return logSize_; }
    int64_t nextOffset() const noexcept { return nextOffset_; }
    std::string logPath() const { return logPath_; }
private:
    static std::string fname(std::string dir, int64_t base, const char* ext);
    /// Shared write core: update index, write bytes, advance nextOffset_.
    /// Does NOT call sync(). Called by append() and appendNoSync().
    int64_t writeAndUpdateState(const uint8_t* data, std::size_t len, int64_t lastOffset);
    int64_t baseOffset_; int32_t indexIntervalBytes_;
    std::string logPath_; std::fstream log_;
    int fd_ = -1;   // POSIX fd for fdatasync — mirrors log_ (write path uses fstream)
    std::unique_ptr<OffsetIndex> index_;
    int64_t logSize_ = 0; int64_t nextOffset_; int64_t bytesSinceIndex_ = 0;
};
} // namespace rune::storage
