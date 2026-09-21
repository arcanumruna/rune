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
    ///
    /// APPEND-THREAD ONLY: this overload reads the segment base offsets via
    /// Log::segmentBaseOffsets() (which iterates the writer-owned segments_ map)
    /// and must therefore only be called on the single-writer append thread.
    /// The cleaner thread MUST use buildOffsetMapFromBases() with snapshot-
    /// derived bases + dir (no segments_ access).
    void buildOffsetMap(Log& log, Offset fromOffset, Offset toOffset, OffsetMap& map);

    /// CP-6 race-fix: pure-file-work offset-map build for the CLEANER THREAD.
    ///
    /// Identical scan to buildOffsetMap() but takes the segment base offsets and
    /// directory EXPLICITLY (derived from the immutable sealed-segment snapshot)
    /// so it never touches Log::segments_.  Reads segment bytes directly from the
    /// on-disk .log files (path = <dir>/<base>.log).  Safe to call concurrently
    /// with the append thread's segments_ mutations.
    ///
    /// `bases` must be sorted ascending and cover at least the sealed segments
    /// overlapping [fromOffset, toOffset).  `toOffset` is used both as the record
    /// filter boundary and as the synthetic "end" of the last base in `bases`.
    void buildOffsetMapFromBases(const std::string& dir,
                                 const std::vector<int64_t>& bases,
                                 Offset fromOffset, Offset toOffset,
                                 OffsetMap& map);

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

    /// CP-2: bounded, single-segment compaction (the duty-cycle unit of work).
    ///
    /// Compacts EXACTLY the one sealed segment whose base offset == `segBase`,
    /// but computes latest-per-key GLOBALLY over the whole dirty range
    /// [globalDirtyStart, activeSegmentBase) so cross-segment supersession is
    /// honoured.  Unlike clean(), this does NOT call the O(n) full-log
    /// reloadSegments(): it reloads ONLY the rewritten segment in place via
    /// Log::reloadSegment(segBase), so a per-segment duty-cycle loop is O(n) total
    /// rather than O(n^2).  The active segment is never `segBase` (the caller passes
    /// a sealed segment) and records at/past segEnd are left untouched.
    ///
    /// @param globalDirtyStart  Start of the WHOLE dirty range (the cleaned
    ///        watermark).  The key->latest-offset map is built over
    ///        [globalDirtyStart, activeSegmentBase) — the ENTIRE dirty region,
    ///        NOT just this segment — so latest-per-key is GLOBAL: a record in
    ///        this segment that is superseded by a later sealed segment is
    ///        correctly dropped.  Only the ONE segment at segBase is rewritten.
    /// @returns true if a compaction rewrite ran (segment existed and was
    ///          processed); false if the segment was absent / already gone.
    bool cleanSegment(Log& log, Offset segBase, Offset segEnd,
                      Offset globalDirtyStart,
                      int64_t nowNs, int64_t deleteRetentionMs);

    /// CP-6 race-fix: CLEANER-THREAD compaction — PURE FILE WORK, no segments_.
    ///
    /// Produces the compacted `<segBase>.log` on disk (atomic write→rename, or a
    /// delete when the segment is fully emptied) WITHOUT touching the writer-owned
    /// Log::segments_ map: it never calls segmentBaseOffsets(), segmentSize() or
    /// reloadSegment().  It builds the global offset map and reads segment bytes
    /// EXCLUSIVELY from the on-disk files, using the base offsets carried in the
    /// immutable sealed-segment snapshot (`sealedBases`) and the immutable log
    /// directory (`dir`, obtained via Log::dir() which is race-free).
    ///
    /// The in-memory reload of the rewritten segment is the caller's
    /// responsibility and MUST happen on the single-writer append thread:
    ///   • LogCleanerAgent (cleaner thread) calls produceCleanedFile(), then
    ///     enqueues segBase via Log::enqueueCleanedSegment(); the append thread's
    ///     drainCleanerSwaps() performs reloadSegment(segBase).
    ///
    /// @param dir          Log directory (immutable; from Log::dir()).
    /// @param sealedBases  Sorted-ascending base offsets of the sealed segments,
    ///                     derived from the sealed-segment snapshot.  Used only to
    ///                     bound the offset-map scan; segments_ is never read.
    /// @param segBase      The single sealed segment to rewrite.
    /// @param activeBase   Upper bound of the dirty range (== active segment base,
    ///                     conservatively the high-watermark).  Records at/past
    ///                     this offset are left untouched.
    /// @param globalDirtyStart  Start of the whole dirty range (cleaned watermark).
    /// @returns true if a rewrite ran (segment file existed and was processed).
    bool produceCleanedFile(const std::string& dir,
                            const std::vector<int64_t>& sealedBases,
                            Offset segBase, Offset activeBase,
                            Offset globalDirtyStart,
                            int64_t nowNs, int64_t deleteRetentionMs);

private:
    /// Is a record a tombstone?
    /// Treats empty value (vlen == 0) as a delete marker, matching Kafka behaviour.
    /// Note: the protocol vlen==-1 (null value) is decoded by RecordBatchReader as
    /// an empty string_view, so this check covers both the null-value and zero-length
    /// cases transparently. The builder never emits a literal vlen==-1 in the output.
    static bool isTombstone(std::string_view value) noexcept {
        return value.empty();
    }

    /// Rewrite a single segment, keeping only records whose offset matches the
    /// latest entry in `map` (or is past cleanBoundaryOffset).  Tombstones
    /// past their delete-retention window are dropped.
    /// PURE FILE WORK: reads/writes only <dir>/<segBase>.log{,.new,.index};
    /// never touches Log::segments_.  Returns true if a file was produced (the
    /// segment was non-empty), false if the source .log was missing/empty.
    bool rewriteSegment(const std::string& dir, int64_t segBase, OffsetMap& map,
                        Offset fromOffset, Offset cleanBoundaryOffset,
                        int64_t nowNs, int64_t deleteRetentionMs);
};

} // namespace rune::storage
