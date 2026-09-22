#pragma once
#include <rune/storage/LogSegment.h>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <utility>
#include <vector>
namespace rune::storage {

/// Immutable descriptor for a single sealed segment — published via atomic
/// snapshot so the cleaner thread can read segment sizes without touching the
/// writer-owned segments_ map.
struct SegInfo {
    int64_t baseOffset      = 0;
    int64_t sizeBytes       = 0;
    /// CP-8: largest record timestamp seen in this segment (epoch-ms or ns,
    /// matching the units used by the producer).  Scanned at seal time from
    /// the batch headers (maxTimestamp field).  Used by minCompactionLagMs
    /// gating on the cleaner thread — no segments_ access required.
    /// 0 = unknown / not yet scanned (treats segment as very old = cleanable).
    int64_t largestTimestamp = 0;
};
using SealedSegmentSnapshot = std::vector<SegInfo>;

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

    /// CP-6/race-fix: Load the latest immutable snapshot of sealed segments.
    ///
    /// The snapshot is a std::shared_ptr<const SealedSegmentSnapshot> published
    /// via std::atomic on the single-writer append thread whenever segments_ changes
    /// (in maybeRoll or after drainCleanerSwaps).  Sealed-segment sizes are fixed
    /// once sealed so the snapshot is stable.
    ///
    /// The cleaner thread MUST use this accessor instead of calling segmentSize()
    /// or segmentBaseOffsets() directly, which would race with the append thread's
    /// mutations to segments_ (UB confirmed by TSan).
    ///
    /// Thread-safe: std::atomic_load provides seq_cst barrier between the
    /// append-thread store (publishSealedSnapshot) and the cleaner-thread load.
    std::shared_ptr<const SealedSegmentSnapshot> sealedSegmentSnapshot() const noexcept {
        return std::atomic_load(&sealedSnapshot_);
    }

    /// CP-6: Enqueue a cleaner-rewritten segment base for atomic swap on the
    /// append/duty-cycle thread.  Called by LogCleanerAgent (the single cleaner
    /// thread) after atomically renaming <base>.log.new → <base>.log on disk.
    /// The actual in-memory reload is deferred to drainCleanerSwaps() which runs
    /// on the single-writer append thread — so no lock is needed on the segment
    /// map.
    ///
    /// Lock-free SPSC: this is the SINGLE-PRODUCER half of a wait-free ring.
    /// The release-store of tail_ publishes the base AND establishes
    /// happens-before with the append thread's acquire-load in drainCleanerSwaps
    /// — this is the release/acquire barrier between the cleaner's file rename
    /// and the append thread's reloadSegment().
    /// Returns false (drops) only if the ring is momentarily full; the cleaner
    /// re-enqueues the same base next cycle (idempotent — reloadSegment is too).
    bool enqueueCleanedSegment(int64_t base) noexcept;

    /// CP-6: Drain pending cleaner swaps on the single-writer append/duty-cycle
    /// thread.  Called once per duty cycle from NodeRuntimeAgent::doWork.
    /// For each pending base calls reloadSegment(base) to atomically update the
    /// in-memory segment map.  The superseded LogSegment is freed here (the map
    /// slot is on the append thread; no reader can hold it after this call since
    /// all reads also happen on the append thread — single-writer invariant).
    ///
    /// Lock-free SPSC: this is the SINGLE-CONSUMER half.  The acquire-load of
    /// tail_ pairs with enqueueCleanedSegment's release-store.
    /// Returns number of swaps processed (>0 = work done).
    int drainCleanerSwaps();

    // -----------------------------------------------------------------------
    // CP-9: Retention async-delete API (single-writer invariant preserved).
    //
    // The cleaner agent (off the append thread) selects segments eligible for
    // deletion, renames them to .deleted on disk (pure file work — OK on the
    // cleaner thread), and enqueues the base via enqueueDeletedSegment().
    //
    // The append/duty-cycle thread calls drainRetentionDeletes(), which erases
    // the segment from segments_ (the ONLY mutation path, preserving the
    // single-writer invariant).
    //
    // Physical removal of .deleted files is deferred: the cleaner calls
    // physicallyRemoveExpiredDeletedFiles(nowMs, fileDeleteDelayMs) once per
    // retention cycle to sweep any .deleted files whose rename time has elapsed.
    //
    // Crash-safety: Log construction scans for .deleted files left by a prior
    // crash and records them for immediate physical cleanup (they were already
    // absent from segments_ since their .log was renamed before crash, so they
    // cannot be re-opened by a restart).
    // -----------------------------------------------------------------------

    /// CP-9 SPSC producer (cleaner thread): enqueue a base for retention deletion.
    /// The .log file has ALREADY been renamed to .deleted on disk (pure file
    /// work done by the cleaner before calling this).
    /// Returns false (drops) if the ring is momentarily full; cleaner retries
    /// next cycle (idempotent — drainRetentionDeletes is a no-op for an unknown
    /// base).
    bool enqueueDeletedSegment(int64_t base) noexcept;

    /// CP-9 SPSC consumer (append thread): drain retention-delete enqueues.
    /// For each pending base, erases the segment from segments_ and updates the
    /// sealed snapshot.  Called from NodeRuntimeAgent::doWork (same thread as
    /// all segments_ mutations).
    /// Returns number of segments erased from segments_.
    int drainRetentionDeletes();

    /// CP-9: physically remove .deleted files whose rename epoch is older than
    /// fileDeleteDelayMs.  Called by the cleaner agent once per retention cycle.
    /// Pure file I/O (fs::remove) — safe on the cleaner thread.
    /// @param nowMs            current wall-clock epoch-ms.
    /// @param fileDeleteDelayMs  delay (ms) before physical removal.
    void physicallyRemoveExpiredDeletedFiles(int64_t nowMs, int64_t fileDeleteDelayMs) noexcept;

    /// CP-9: Record a .deleted file for deferred physical removal.
    /// Called by the cleaner after renaming .log → .deleted.
    /// Thread-safe: the cleaner thread is the only caller; the list is read
    /// back only in physicallyRemoveExpiredDeletedFiles (also cleaner thread).
    void recordDeletedFile(const std::string& deletedPath, int64_t renamedAtMs) noexcept;

private:
    LogSegment& activeSegment() { return *segments_.rbegin()->second; }
    void maybeRoll();

    /// Rebuild and atomically publish a new sealed-segment snapshot.
    /// MUST be called on the single-writer append thread whenever segments_ changes:
    ///   - after maybeRoll() (when a segment seals)
    ///   - after drainCleanerSwaps() (after a segment swap)
    /// Only sealed segments (all except the last/active) are included; the active
    /// segment's size is mutable so it is excluded.
    void publishSealedSnapshot();

    std::string dir_; int64_t rollBytes_; int64_t logEndOffset_ = 0;
    /// CP-8 incremental largestTimestamp: running max of maxTimestamp() across all
    /// batches appended to the ACTIVE segment since the last roll.  Reset to 0 at
    /// each roll (after publishSealedSnapshot captures it for the newly-sealed
    /// segment).  Updated on every append() / appendNoSync() call so publishSeal-
    /// edSnapshot never needs to re-scan the file for a freshly-sealed segment.
    int64_t activeSegmentMaxTs_ = 0;
    /// CP-8: max-ts for the MOST RECENTLY SEALED segment, captured from
    /// activeSegmentMaxTs_ in maybeRoll() just before the reset.  Consumed by
    /// publishSealedSnapshot() to populate the newly-sealed SegInfo entry without
    /// any file I/O.  Only meaningful on the append thread (where maybeRoll and
    /// publishSealedSnapshot are always called in sequence).
    int64_t newlySealedMaxTs_ = 0;
    std::map<int64_t, std::unique_ptr<LogSegment>> segments_;

    // CP-6/race-fix: atomic snapshot of sealed segment {baseOffset, sizeBytes}.
    // Written exclusively on the append thread; read safely on the cleaner thread
    // via std::atomic_store / std::atomic_load (deprecated C++20 free-function
    // API, but the only portable form available on Apple clang/libc++ 17 where
    // std::atomic<shared_ptr<T>> is not yet fully supported).
    //
    // Correctness: atomic_store/load use an internal spinlock in libc++, providing
    // a full happens-before between the store on the writer and the load on the
    // cleaner — equivalent in strength to a mutex-protected assignment.  The
    // spinlock is per-pointer-address (not global), so the append hot path is
    // only affected when a roll occurs (infrequent).
    mutable std::shared_ptr<const SealedSegmentSnapshot> sealedSnapshot_{
        std::make_shared<SealedSegmentSnapshot>()};

    // CP-6: pending cleaner swaps — LOCK-FREE single-producer/single-consumer
    // ring.  Producer = the single cleaner thread (enqueueCleanedSegment);
    // consumer = the single append/duty-cycle thread (drainCleanerSwaps).
    // No mutex on any path.
    //
    //   head_ : consumer-owned index (only the append thread advances it).
    //   tail_ : producer-owned index (only the cleaner thread advances it).
    // A release-store to tail_ publishes a slot; an acquire-load of tail_ on the
    // consumer establishes happens-before with the cleaner's file rename.
    // Symmetrically head_ (release on consumer / acquire on producer) frees slots.
    // Capacity is a power of two so index masking is a bitwise AND.
    static constexpr std::size_t kSwapRingCapacity = 1024; // power of two
    static constexpr std::size_t kSwapRingMask     = kSwapRingCapacity - 1;
    std::array<int64_t, kSwapRingCapacity> swapRing_{};
    alignas(64) std::atomic<std::size_t>   swapHead_{0}; // consumer index
    alignas(64) std::atomic<std::size_t>   swapTail_{0}; // producer index

    // CP-9: pending retention deletions — separate SPSC ring (to avoid mixing
    // compaction swaps with retention deletes; they have different semantics).
    // Producer = cleaner thread (enqueueDeletedSegment, after .log→.deleted rename).
    // Consumer = append/duty-cycle thread (drainRetentionDeletes → segments_.erase).
    static constexpr std::size_t kDeleteRingCapacity = 1024;
    static constexpr std::size_t kDeleteRingMask     = kDeleteRingCapacity - 1;
    std::array<int64_t, kDeleteRingCapacity> deleteRing_{};
    alignas(64) std::atomic<std::size_t>    deleteHead_{0}; // consumer index
    alignas(64) std::atomic<std::size_t>    deleteTail_{0}; // producer index

    // CP-9: deferred physical-removal list — pairs of (deletedFilePath, renamedAtMs).
    // Written by the cleaner (recordDeletedFile) and read/cleared by
    // physicallyRemoveExpiredDeletedFiles.  Both are called ONLY on the cleaner
    // thread so no synchronization is needed.
    std::vector<std::pair<std::string, int64_t>> pendingPhysicalDeletes_;
};
} // namespace rune::storage
