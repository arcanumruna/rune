#pragma once
#include <rune/storage/Log.h>
#include <rune/storage/OffsetMap.h>
#include <cstdint>

namespace rune::storage {

/// Kafka-style log compaction for a single Log.
///
/// Mirrors Kafka's LogCleaner semantics:
///  1. buildOffsetMap — scan the dirty portion [fromOffset, toOffset) of the
///     log, recording key→latest-offset in an OffsetMap.  A tombstone (null/
///     empty value, vlen == -1 per the protocol) sets the key's entry to that
///     tombstone's offset (so all earlier values are dropped).
///  2. compact — rewrite segments in the range, keeping ONLY records whose
///     offset == offsetMap.get(key) (the latest); drop superseded records; a
///     tombstone is retained until (nowNs - tombstoneTimestampNs) >
///     deleteRetentionMs * 1_000_000; after that it is removed entirely.
///     Original offsets are PRESERVED (gaps are expected — offsets are NOT
///     renumbered).  The active segment (the highest-base-offset segment) is
///     NEVER compacted.  Records at or past `cleanBoundaryOffset` (the
///     high-watermark / first-dirty-offset) are also untouched.
///
/// Crash safety: each segment is rewritten to a ".log.new" temporary file,
/// then atomically renamed over the original ".log" (+ new ".index").  A crash
/// mid-compaction leaves either the old or new segment intact — on reopen
/// Log::recover() reads whichever is present and valid.  Stale ".log.new" temps
/// are cleaned up at the start of compact().
class LogCleaner {
public:
    LogCleaner() = default;

    /// Scan log batches whose base-offset falls in [fromOffset, toOffset) and
    /// populate `map` with key→latest-offset entries.
    /// A tombstone record (value == "" or vlen < 0 in the wire format) is also
    /// recorded — its offset represents "latest delete intent".
    void buildOffsetMap(Log& log, Offset fromOffset, Offset toOffset, OffsetMap& map);

    /// Compact the log up to `cleanBoundaryOffset` using a previously-built
    /// OffsetMap.
    ///
    /// @param log                 The log to compact.
    /// @param map                 Key→latest-offset map built by buildOffsetMap.
    /// @param firstDirtyOffset    Start of the dirty range (= the `fromOffset`
    ///                            passed to buildOffsetMap).  Records at offset
    ///                            < firstDirtyOffset are in the clean region and
    ///                            are always preserved verbatim.
    /// @param cleanBoundaryOffset Records at offset >= cleanBoundaryOffset are
    ///                            left untouched (HW / active-segment guard).
    /// @param nowNs               Current monotonic time in nanoseconds.
    /// @param deleteRetentionMs   How long (ms) to retain tombstones before
    ///                            removing them entirely.
    void compact(Log& log, OffsetMap& map,
                 Offset firstDirtyOffset, Offset cleanBoundaryOffset,
                 int64_t nowNs, int64_t deleteRetentionMs);

    /// Convenience: build the offset map and compact in one call.
    /// The dirty range is [log.firstDirtyOffset, cleanBoundaryOffset).
    /// `firstDirtyOffset` is typically 0 for a fresh compaction.
    void clean(Log& log, Offset firstDirtyOffset, Offset cleanBoundaryOffset,
               int64_t nowNs, int64_t deleteRetentionMs);

private:
    /// Is a record a tombstone?  The protocol encodes null/empty value as
    /// vlen == -1; we also treat vlen == 0 (empty string) as a tombstone for
    /// compaction purposes (matches Kafka behaviour: empty value = delete marker).
    static bool isTombstone(std::string_view value) noexcept {
        return value.empty();
    }

    /// Rewrite a single segment, keeping only records whose offset matches the
    /// latest entry in `map` (or is past cleanBoundaryOffset).  Tombstones
    /// past their delete-retention window are dropped.
    /// Returns the path of the rewritten temp file (already renamed in place).
    void rewriteSegment(Log& log, int64_t segBase, OffsetMap& map,
                        Offset fromOffset, Offset cleanBoundaryOffset,
                        int64_t nowNs, int64_t deleteRetentionMs);
};

} // namespace rune::storage
