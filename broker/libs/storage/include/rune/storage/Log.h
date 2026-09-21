#pragma once
#include <rune/storage/LogSegment.h>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <vector>
namespace rune::storage {

/// On-disk append-only log: a sequence of LogSegment files in a directory.
///
/// IMPORTANT: A Log constructed by the default constructor is NOT fully usable
/// until recover() is called. Before recover(), logEndOffset() returns the base
/// offset of the last segment (not the true LEO) and the in-memory index for
/// each segment is empty (reads fall back to O(segment) scan from position 0).
/// PartitionApi always calls recover() — any other user must do the same.
class Log {
public:
    Log(std::string dir, int64_t segmentRollBytes = 64LL*1024*1024);
    /// Zero-copy span overload — avoids per-call vector construction on the hot path.
    /// Appends [data, data+len) as a batch with the given lastOffset.
    /// Calls sync() after writing (durable-by-default).
    int64_t append(const uint8_t* data, std::size_t len, int64_t lastOffset);
    /// Vector overload (kept for compatibility; delegates to the span overload).
    int64_t append(const std::vector<uint8_t>& batch, int64_t lastOffset);

    /// No-sync append — writes to page cache only; does NOT call sync().
    ///
    /// Gateway group-commit path: use appendNoSync() for every per-produce
    /// write, then call sync() once per duty-cycle to amortise the fsync over
    /// all produces in that cycle.
    ///
    /// Durability contract (acks=Leader / acks=1):
    ///   - Data immediately visible to Log::read() (page cache).
    ///   - NOT fsync-durable: a leader crash before sync() may lose un-flushed
    ///     acked records.  True crash-durability = acks=All + ISR replication
    ///     (Milestone C).
    ///
    /// Callers MUST NOT advance the durable ISR high-watermark past an offset
    /// before sync() has returned for that offset.
    int64_t appendNoSync(const uint8_t* data, std::size_t len, int64_t lastOffset);
    std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes);
    int64_t logEndOffset() const noexcept { return logEndOffset_; }
    int64_t recover();
    /// Flush + fdatasync the active segment's .log and msync its .index.
    /// append() already calls this (durable-by-default). External group-commit
    /// callers may batch appends and call sync() once — but MUST NOT advance
    /// the high-watermark past an offset before sync() returns for that offset.
    void sync();
    /// Truncate all records with baseOffset >= truncateOffset.
    /// The log's logEndOffset is reset to truncateOffset.
    /// Segments entirely beyond truncateOffset are deleted.
    /// The active segment is physically truncated at the correct byte boundary.
    void truncateTo(int64_t truncateOffset);
    std::vector<int64_t> segmentBaseOffsets() const;
    int64_t totalBytes() const;
    int deleteOldSegments(int64_t retainBytes);
    /// Directory that contains this log's segment files.
    const std::string& dir() const noexcept { return dir_; }
    /// Reload all segments from disk (close + reopen every LogSegment).
    /// Used by LogCleaner after atomically rewriting segment files on disk,
    /// so that in-memory file handles reflect the new file contents.
    void reloadSegments();
    /// CP-2: reload a SINGLE sealed segment in place (close + reopen just that
    /// LogSegment) after its .log file was atomically rewritten by the cleaner.
    /// O(1) in the number of segments — avoids the O(n) full-log reloadSegments()
    /// that re-recover()s every segment (which makes per-segment compaction O(n^2)).
    /// If the segment's .log file no longer exists on disk (the cleaner emptied and
    /// deleted it), the segment is dropped from the in-memory map.  MUST NOT be
    /// called for the active segment and does NOT change logEndOffset_ (sealed-
    /// segment rewrites never move the LEO).
    void reloadSegment(int64_t baseOffset);
private:
    LogSegment& activeSegment() { return *segments_.rbegin()->second; }
    void maybeRoll();
    std::string dir_; int64_t rollBytes_; int64_t logEndOffset_ = 0;
    std::map<int64_t, std::unique_ptr<LogSegment>> segments_;
};
} // namespace rune::storage
