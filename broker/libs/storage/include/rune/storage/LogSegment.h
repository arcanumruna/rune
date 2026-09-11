#pragma once
#include <rune/storage/OffsetIndex.h>
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
class LogSegment {
public:
    LogSegment(std::string dir, int64_t baseOffset, int32_t indexIntervalBytes = 4096);
    ~LogSegment();
    int64_t append(const std::vector<uint8_t>& batch, int64_t lastOffset); // returns nextOffset

    /// Flush libc buffer + fdatasync ".log" + msync ".index".
    /// Called automatically by append() (durable-by-default).
    /// External callers (e.g. group-commit) may call this explicitly after
    /// batching multiple appends, provided no offset is ack'd before sync().
    void sync();

    std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes);
    int64_t recover();

    int64_t baseOffset() const noexcept { return baseOffset_; }
    int64_t sizeBytes() const noexcept { return logSize_; }
    int64_t nextOffset() const noexcept { return nextOffset_; }
    std::string logPath() const { return logPath_; }
private:
    static std::string fname(std::string dir, int64_t base, const char* ext);
    int64_t baseOffset_; int32_t indexIntervalBytes_;
    std::string logPath_; std::fstream log_;
    int fd_ = -1;   // POSIX fd for fdatasync — mirrors log_ (write path uses fstream)
    std::unique_ptr<OffsetIndex> index_;
    int64_t logSize_ = 0; int64_t nextOffset_; int64_t bytesSinceIndex_ = 0;
};
} // namespace rune::storage
