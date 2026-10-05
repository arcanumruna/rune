#pragma once
#include <rune/storage/Log.h>
#include <rune/storage/OffsetMap.h>
#include <rune/storage/SkimpyOffsetMap.h>
#include <rune/storage/Throttler.h>
#include <cstdint>
#include <functional>
#include <vector>

namespace rune::storage {

/// CP-8: A contiguous group of sealed segments to be coalesced into one
/// cleaned output segment by groupSegmentsBySize().
struct SegmentGroup {
    std::vector<int64_t> bases; ///< Sorted ascending base offsets of the input segments.
    int64_t combinedBytes = 0;  ///< Approximate combined output size (upper bound).
};


/// Result of a bounded-range compaction pass.
/// `compacted` = true if a rewrite ran; `passEnd` = the first-dirty-offset
/// for the NEXT pass (== activeBase when the map covered the whole range, or
/// map.latestOffset()+1 when the map filled before covering everything).
struct CleanPassResult {
    bool    compacted = false;
    Offset  passEnd   = -1; ///< Next pass should start from here.
};

/// Result of a from-log-start range compaction pass (produceCleanedRangeBounded).
/// `passEnd` is the next-pass watermark (map coverage clamp).  `rewrittenBases`
/// lists ONLY the segments whose on-disk .log actually changed this pass (dropped
/// records or deleted) — the caller enqueues a swap for exactly these; unchanged
/// prefix segments are left untouched and are NOT enqueued.
/// Fix round 2 (deferred install): one compaction output the OWNER thread must
/// validate and install.  `bases` = the replaced segments (ascending; a merged
/// group or a single rewrite).  hasOutput: the fsynced output is at
/// Log::cleanedLogPath(dir, bases.front(), seq); false = every record was
/// removed (the members are simply dropped).  The originals are untouched.
struct CleanedGroup {
    std::vector<int64_t> bases;
    uint64_t             seq       = 0;
    bool                 hasOutput = false;
};

struct RangeCleanResult {
    bool                 compacted = false;
    Offset               passEnd   = -1;
    std::vector<int64_t> rewrittenBases;
    /// Deferred-install mode only (setDeferredInstall): the outputs to hand to
    /// the owner thread (Log::enqueueCleanedGroup).  rewrittenBases still lists
    /// every member base.
    std::vector<CleanedGroup> groups;
};

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
    /// I1 (Kafka producer.id.expiration.ms): a producer whose last batch's
    /// maxTimestamp is older than now - this is no longer retained by
    /// compaction (ProducerTails.h).  Default 7 days; < 0 = never expire.
    void setProducerIdExpirationMs(int64_t ms) noexcept { producerIdExpirationMs_ = ms; }
    [[nodiscard]] int64_t producerIdExpirationMs() const noexcept { return producerIdExpirationMs_; }
    /// M3: bytes the last offset-map build read from clean-prefix segments
    /// (producer tails) — throttled by the bounded passes.
    [[nodiscard]] int64_t lastPrefixBytesRead() const noexcept { return prefixBytesRead_; }

    LogCleaner() = default;

    /// D3b fix 1: a COMMIT/ABORT marker may be removed (horizon-stamped, then
    /// dropped) only below this offset — the minimum fetch position over EVERY
    /// assigned replica (PartitionReplica::cleanerMarkerRemovalBound): a lagging
    /// replica that holds a txn's data but not yet its marker must still receive
    /// the marker, or after a failover it would treat the aborted data as part of
    /// the producer's next txn.  Default 0 = never remove a marker (a caller with
    /// no replica knowledge keeps them all — correctness over growth).
    void setMarkerRemovalBound(Offset bound) noexcept { markerRemovalBound_ = bound; }
    [[nodiscard]] Offset markerRemovalBound() const noexcept { return markerRemovalBound_; }
    /// Segment files the rewrite phase of the last pass read (test/accounting).
    [[nodiscard]] int64_t lastRewriteSegmentsRead() const noexcept { return rewriteSegmentsRead_; }

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

    /// CP-7: BOUNDED overload — uses a SkimpyOffsetMap with a hard slot limit.
    ///
    /// Identical to the OffsetMap overload BUT stops scanning as soon as
    /// `map.isFull()` returns true (bounded-range-per-pass).  The caller MUST
    /// check `map.latestOffset()` after this call to determine the pass boundary:
    ///   passEnd = map.latestOffset() + 1;
    /// then compact only segments with base < passEnd, and advance the
    /// cleaner checkpoint to passEnd for the next cycle.
    ///
    /// Returns the highest offset that was successfully entered into the map,
    /// or -1 if no records were scanned.  This is identical to
    /// map.latestOffset() after the call.
    Offset buildOffsetMapFromBases(const std::string& dir,
                                   const std::vector<int64_t>& bases,
                                   Offset fromOffset, Offset toOffset,
                                   SkimpyOffsetMap& map);

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
    /// @param nowMs               Wall-clock epoch-ms (KIP-534 delete horizon for
    ///                            tombstones AND removable txn markers).  -1 =
    ///                            legacy nowNs tombstone path; markers are kept.
    void compact(Log& log, OffsetMap& map,
                 Offset firstDirtyOffset, Offset cleanBoundaryOffset,
                 int64_t nowNs, int64_t deleteRetentionMs, int64_t nowMs = -1);

    /// Convenience: build the offset map and compact in one call.
    /// The dirty range is [log.firstDirtyOffset, cleanBoundaryOffset).
    /// `firstDirtyOffset` is typically 0 for a fresh compaction.
    void clean(Log& log, Offset firstDirtyOffset, Offset cleanBoundaryOffset,
               int64_t nowNs, int64_t deleteRetentionMs, int64_t nowMs = -1);

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
    /// Legacy overload: uses unbounded OffsetMap.  Used by cleanSegment()
    /// (append-thread path) and legacy tests.  Returns true if a rewrite ran.
    ///
    /// @param dir          Log directory (immutable; from Log::dir()).
    /// @param sealedBases  Sorted-ascending base offsets of the sealed segments.
    /// @param segBase      The single sealed segment to rewrite.
    /// @param activeBase   Upper bound of the dirty range.
    /// @param globalDirtyStart  Start of the whole dirty range.
    bool produceCleanedFile(const std::string& dir,
                            const std::vector<int64_t>& sealedBases,
                            Offset segBase, Offset activeBase,
                            Offset globalDirtyStart,
                            int64_t nowNs, int64_t deleteRetentionMs);

    /// CP-7: BOUNDED version of produceCleanedFile — uses SkimpyOffsetMap
    /// sized by `dedupeBufferBytes`.
    ///
    /// The key difference from the unbounded overload: the offset-map scan
    /// stops when the SkimpyOffsetMap is FULL (bounded-range-per-pass).  The
    /// returned `passEnd` reflects the highest offset the map covered + 1.
    /// The caller MUST advance the cleaner watermark to `passEnd` (not to
    /// `activeBase`), and must compact only the segment if `segBase < passEnd`.
    ///
    /// When the map covers the entire [globalDirtyStart, activeBase) range
    /// without filling up, `passEnd == activeBase` (full-pass, no residue).
    ///
    /// @param dedupeBufferBytes  Memory budget for the SkimpyOffsetMap.
    ///                           Must be > 0.  Sized via
    ///                           SkimpyOffsetMap::maxSlotsFromBudget().
    /// @param throttler          Optional shared I/O throttler (may be nullptr).
    ///                           Called after each segment file read/write.
    ///                           MUST be called only on the cleaner thread.
    /// @param nowMs              Current wall-clock epoch-ms (sampled once per pass
    ///                           by the caller — deterministic).  Used for delete-
    ///                           horizon stamping/dropping.
    /// @param deleteRetentionMs  As before — how long to retain tombstones.
    ///
    /// CLEANER THREAD: PURE FILE WORK — never touches Log::segments_.
    CleanPassResult produceCleanedFileBounded(const std::string& dir,
                                              const std::vector<int64_t>& sealedBases,
                                              Offset segBase, Offset activeBase,
                                              Offset globalDirtyStart,
                                              int64_t nowNs, int64_t deleteRetentionMs,
                                              int64_t dedupeBufferBytes,
                                              Throttler* throttler = nullptr,
                                              int64_t nowMs = -1);

    /// CONVERGENCE FIX: Kafka `doClean`-from-log-start range compaction.
    ///
    /// Builds ONE bounded SkimpyOffsetMap over the dirty section
    /// [globalDirtyStart, activeBase), then re-copies EVERY sealed segment whose
    /// base < passEnd (i.e. the whole log prefix + dirty section up to the map's
    /// coverage boundary) against that map.  This is the load-bearing correctness
    /// property that produceCleanedFileBounded (single dirty-tail segment) lacked:
    /// a key written BEFORE the cleaner watermark and rewritten AFTER it has its
    /// stale prefix copy dropped here, so compaction converges to exactly one live
    /// record per key even when producers interleave with the cleaner (drcp1/3/4).
    ///
    /// Only segments whose content actually changed are returned in
    /// `rewrittenBases` (via the rewriteSegmentImpl `changed` out-param) — the
    /// caller enqueues a swap for exactly those; already-clean prefix segments are
    /// left untouched on disk and are NOT enqueued (no churn).
    ///
    /// `passEnd` == activeBase when the map covered the whole dirty range; it is
    /// map.latestOffset()+1 (< activeBase) when the bounded map filled first — the
    /// caller advances the watermark to passEnd and the next pass resumes there.
    ///
    /// CLEANER THREAD: PURE FILE WORK — never touches Log::segments_.
    /// @param sealedSizes   Parallel to sealedBases; on-disk file sizes.  Used only
    ///                      when useCoalescing=true, to group adjacent segments.
    /// @param useCoalescing When true, adjacent in-pass segments are merged into
    ///                      one output (Kafka segment grouping) via mergeGroupWithMap;
    ///                      when false, each in-pass segment is rewritten in place.
    /// @param groupRollBytes  Max combined output bytes per coalesced group.
    /// @param map  REUSABLE dedupe map (sized to the budget once by the caller);
    ///             cleared and refilled here so the backing array is not
    ///             re-allocated every pass (ULL: near-zero per-pass heap traffic).
    RangeCleanResult produceCleanedRangeBounded(const std::string& dir,
                                                const std::vector<int64_t>& sealedBases,
                                                const std::vector<int64_t>& sealedSizes,
                                                Offset activeBase,
                                                Offset globalDirtyStart,
                                                int64_t nowNs, int64_t deleteRetentionMs,
                                                SkimpyOffsetMap& map,
                                                bool useCoalescing,
                                                int64_t groupRollBytes,
                                                Throttler* throttler = nullptr,
                                                int64_t nowMs = -1);

    /// CP-8: Group consecutive sealed segments into coalesced groups for compaction.
    ///
    /// Mirrors Kafka's LogCleaner.groupSegmentsBySize:
    ///   - Accumulate consecutive sealed segments into a group while:
    ///       (a) combined data ≤ segmentRollBytes (output fits in one segment)
    ///       (b) combined index entries ≤ maxIndexEntries (index size limit)
    ///       (c) offset span ≤ INT32_MAX (4-byte relative offset in OffsetIndex)
    ///   - Each group produces ONE cleaned output segment in the cleaner.
    ///   - Segments in [firstUncleanableIdx .. end) are excluded.
    ///
    /// @param sealedBases      Sorted-ascending bases of all CLEANABLE sealed segments
    ///                         (i.e. those with base < firstUncleanable and within
    ///                          the dirty range).
    /// @param sealedSizes      Parallel vector of file sizes (bytes) for each base.
    /// @param segmentRollBytes Max combined output bytes per group.
    /// @param maxIndexEntries  Max offset-index entries per output segment.
    ///                         Default: 4096*1024/8 = 524288 (~4 MiB index at 8 B/entry).
    ///
    /// @returns Vector of groups (each group has >= 1 base).
    ///
    /// PURE COMPUTATION — no file I/O; safe on any thread.
    static std::vector<SegmentGroup>
    groupSegmentsBySize(const std::vector<int64_t>& sealedBases,
                        const std::vector<int64_t>& sealedSizes,
                        int64_t segmentRollBytes,
                        int32_t maxIndexEntries = 524288);

    /// CP-8: Compact an entire group of segments (coalescing) into a SINGLE output
    /// segment at the base of the first segment in the group.
    ///
    /// Reads every segment in `group.bases` and writes a single cleaned output at
    /// `group.bases[0]` (i.e. the group's lowest base offset becomes the output base).
    /// This is the grouped rewrite path: instead of one call per segment, the cleaner
    /// calls this once per group when grouping is enabled.
    ///
    /// The SkimpyOffsetMap is built over the entire dirty range [globalDirtyStart,
    /// activeBase) (bounded by dedupeBufferBytes).  The output segment is written as
    /// a single .log.new file at `dir/<group.bases[0]>.log.new` then renamed over the
    /// first segment, and all other segments in the group are deleted (they've been
    /// merged in).
    ///
    /// @returns CleanPassResult with compacted=true if a rewrite ran, passEnd set.
    ///
    /// CLEANER THREAD: PURE FILE WORK — never touches Log::segments_.
    CleanPassResult produceCleanedGroupBounded(const std::string& dir,
                                               const std::vector<int64_t>& sealedBases,
                                               const SegmentGroup& group,
                                               Offset activeBase,
                                               Offset globalDirtyStart,
                                               int64_t nowNs, int64_t deleteRetentionMs,
                                               int64_t dedupeBufferBytes,
                                               Throttler* throttler = nullptr,
                                               int64_t nowMs = -1);

    /// Fix round 2 (Kafka-style swap on the owner thread): in deferred mode
    /// produceCleanedRangeBounded NEVER renames, replaces or deletes a segment
    /// file — each output is written (fsynced) to a unique
    /// Log::cleanedLogPath(dir, base, seq) and reported in
    /// RangeCleanResult::groups; the owner thread validates and installs it
    /// (Log::drainCleanerSwaps).  `nextSeq` is the caller's cleaner-owned
    /// monotonic counter (unique output names across passes).
    void setDeferredInstall(uint64_t* nextSeq) noexcept { deferredSeq_ = nextSeq; }
    /// Max members per deferred group (bounds the owner's ring claim).
    static constexpr std::size_t kMaxDeferredGroupMembers = 256;

private:
    /// Is a record a tombstone?
    /// Treats empty value (vlen == 0) as a delete marker, matching Kafka behaviour.
    /// Note: the protocol vlen==-1 (null value) is decoded by RecordBatchReader as
    /// an empty string_view, so this check covers both the null-value and zero-length
    /// cases transparently. The builder never emits a literal vlen==-1 in the output.
    static bool isTombstone(std::string_view value) noexcept {
        return value.empty();
    }

    /// Rewrite a single segment using a type-erased key lookup.
    /// `getLatest(key)` returns the latest known offset for the key from the map.
    /// PURE FILE WORK: reads/writes only <dir>/<segBase>.log{,.new,.index};
    /// never touches Log::segments_.  Returns true if a file was produced.
    ///
    /// @param nowMs  Wall-clock epoch-ms sampled once per pass (for KIP-534 horizon).
    ///               Pass -1 to use the legacy nowNs-only tombstone retention path.
    /// @param changed  Optional out-param: set to true iff this pass altered the
    ///                 segment's logical content (dropped a record, dropped or
    ///                 first-stamped a tombstone).  When false, the on-disk .log
    ///                 is left untouched and is byte-valid — the caller should NOT
    ///                 enqueue a swap for this segment.  Nullptr = don't report.
    bool rewriteSegmentImpl(const std::string& dir, int64_t segBase,
                            const std::function<std::optional<Offset>(std::string_view)>& getLatest,
                            Offset fromOffset, Offset cleanBoundaryOffset,
                            int64_t nowNs, int64_t deleteRetentionMs,
                            int64_t nowMs = -1, bool* changed = nullptr,
                            const ProducerTails* tails = nullptr,
                            const TxnCleanState* txn = nullptr);

    /// Rewrite a single segment, keeping only records whose offset matches the
    /// latest entry in `map` (or is past cleanBoundaryOffset).  Tombstones
    /// past their delete-retention window are dropped.
    /// PURE FILE WORK: reads/writes only <dir>/<segBase>.log{,.new,.index};
    /// never touches Log::segments_.  Returns true if a file was produced (the
    /// segment was non-empty), false if the source .log was missing/empty.
    ///
    /// @param nowMs  Wall-clock epoch-ms (for KIP-534 delete-horizon stamping).
    ///               Pass -1 to use the legacy nowNs-only path.
    bool rewriteSegment(const std::string& dir, int64_t segBase, OffsetMap& map,
                        Offset fromOffset, Offset cleanBoundaryOffset,
                        int64_t nowNs, int64_t deleteRetentionMs,
                        int64_t nowMs = -1);

    /// SkimpyOffsetMap overload of rewriteSegment.
    bool rewriteSegment(const std::string& dir, int64_t segBase, SkimpyOffsetMap& map,
                        Offset fromOffset, Offset cleanBoundaryOffset,
                        int64_t nowNs, int64_t deleteRetentionMs,
                        int64_t nowMs = -1);

    /// Coalesce the in-pass segments of one group into the group's lowest base,
    /// filtering every record against the PREBUILT dirty `map` (built once over the
    /// whole dirty range so cross-segment supersession is honoured).  Sets *changed
    /// iff the group's on-disk layout changed.  PURE FILE WORK — no segments_.
    void mergeGroupWithMap(const std::string& dir,
                           const std::vector<int64_t>& groupInPass,
                           SkimpyOffsetMap& map,
                           Offset globalDirtyStart, Offset passEnd,
                           int64_t nowNs, int64_t deleteRetentionMs, int64_t nowMs,
                           Throttler* throttler, bool* changed);
    int64_t producerIdExpirationMs_ = ProducerTails::kDefaultExpirationMs;
    int64_t prefixBytesRead_        = 0;
    Offset  markerRemovalBound_     = 0;
    int64_t rewriteSegmentsRead_    = 0;
    uint64_t* deferredSeq_          = nullptr;   ///< non-null = deferred-install mode
    std::vector<CleanedGroup>* deferredOut_ = nullptr;   ///< set for one pass
    /// Deferred mode: write `out` durably to a fresh cleaned path for `bases`
    /// (or record an all-removed group) — never touches the originals.
    void deferInstall_(const std::string& dir, const std::vector<int64_t>& bases,
                       const std::vector<uint8_t>& out);
};

} // namespace rune::storage
