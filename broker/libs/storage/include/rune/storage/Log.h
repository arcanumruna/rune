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
    /// Exclusive end offset of this sealed segment = the base offset of the NEXT
    /// segment in the log (which is the ACTIVE segment for the last sealed entry).
    /// Lets the cleaner thread compute exact segment boundaries AND the true
    /// first-uncleanable offset (= the active segment base = the last sealed
    /// entry's endOffset) WITHOUT reading the writer-owned segments_ map — so the
    /// cleaner never advances its watermark past not-yet-sealed (active) records.
    int64_t endOffset        = 0;
};
using SealedSegmentSnapshot = std::vector<SegInfo>;

/// On-disk append-only log: a sequence of LogSegment files in a directory.
///
/// IMPORTANT: A Log constructed by the default constructor is NOT fully usable
/// until recover() is called. Before recover(), logEndOffset() returns the base
/// offset of the last segment (not the true LEO) and the in-memory index for
/// each segment is empty (reads fall back to O(segment) scan from position 0).
/// PartitionApi always calls recover() — any other user must do the same.
/// De-virtualized (Milestone D Phase 2): `Log` is an INDEPENDENT concrete class.
/// The former `ILog` seam is gone — `PartitionReplica` holds a concrete `Log*`
/// (or `InMemoryLog*`) and dispatches on a `bool memoryBacked_`, so every
/// hot-path log operation is a direct, inlinable call with NO vtable.
class Log {
public:
    Log(std::string dir, int64_t segmentRollBytes = 64LL*1024*1024);
    ~Log();
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

    /// Zero-copy restamp append (produce hot path).  Assigns the base offset from
    /// the current logEndOffset() and writes [batch, batch+len) to the page cache
    /// with that base offset patched into bytes [0..7] via pwritev scatter-gather
    /// — `batch` is NEVER mutated (may alias a read-only Aeron fragment / ring
    /// slot), so callers need no restamp scratch buffer.  `lastOffsetDelta` is the
    /// LE32 field at kLastOffsetDeltaOffset (recordCount-1).  Sets `outBaseOffset`
    /// to the assigned base offset and returns nextOffset (= base + delta + 1).
    /// Same page-cache durability contract as appendNoSync().
    int64_t appendNoSyncRestamp(const uint8_t* batch, std::size_t len,
                                int32_t lastOffsetDelta, int64_t& outBaseOffset);

    std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes);

    /// Zero-copy read: locate the segment containing startOffset and return a
    /// span into its read-only mmap covering whole record batches (see
    /// LogSegment::readView / LogReadView).  No heap allocation, no copy.  Returns
    /// an empty view if startOffset is below logStartOffset() or at/after the LEO.
    LogReadView readView(int64_t startOffset, int32_t maxBytes);

    int64_t logEndOffset() const noexcept { return logEndOffset_; }
    /// The lowest offset still readable from this log — the base offset of the
    /// first (oldest) on-disk segment.  Advances past 0 when retention/compaction
    /// deletes the earliest segment(s).  An "Earliest" subscriber MUST start here,
    /// NOT at 0: reading below the first segment base returns empty forever
    /// (Log::read's upper_bound lands on begin() and yields {}), which silently
    /// starves the subscriber (0 delivered).  Returns 0 for an empty log.
    int64_t logStartOffset() const noexcept;
    int64_t recover();
    /// Flush + fdatasync the active segment's .log and msync its .index.
    /// append() already calls this (durable-by-default). External group-commit
    /// callers may batch appends and call sync() once — but MUST NOT advance
    /// the high-watermark past an offset before sync() returns for that offset.
    void sync();
    /// Test hook: number of inline LogSegment::sync calls on this log's segments
    /// (append's durable fsync, sync(), the inline seal) — spec §3.1: a shard
    /// loop must make none; its flushes go through the io backend.
    [[nodiscard]] uint64_t syncCallsForTest() const noexcept { return storage_.syncCalls; }
    /// Test hook: while non-zero every inline sync fails with this errno.
    void setSyncFaultForTest(int err) noexcept { storage_.faultForTest = err; }

    // ---- fsyncgate fail-stop (sticky per-log storage failure) -----------------
    /// True once ANY fsync/msync on this log failed (inline or async).  Never
    /// clears: appends throw, Log::sync throws, producers get StorageError and
    /// the broker halts.  Restart recovery truncates to the last valid batch.
    [[nodiscard]] bool storageFailed() const noexcept { return storage_.failedErrno != 0; }
    [[nodiscard]] int  storageErrno()  const noexcept { return storage_.failedErrno; }
    /// Record an async (io backend) fsync failure.  First error wins.
    void markStorageFailed(int err) noexcept {
        if (storage_.failedErrno == 0) storage_.failedErrno = err != 0 ? err : 5 /*EIO*/;
    }

    // ---- async seal (shard mode, spec §3.1 never-block) ----------------------
    /// When on, a roll does NOT fsync the sealed segment inline: its buffered
    /// bytes are pushed to the page cache, the segment is pinned and queued as a
    /// pending seal for the owning worker to fdatasync through its io backend.
    /// Legacy (off): the seal fsync stays inline.  Owner thread only.
    void setDeferSealSync(bool on) noexcept { deferSealSync_ = on; }
    [[nodiscard]] bool deferSealSync() const noexcept { return deferSealSync_; }
    [[nodiscard]] std::size_t pendingSealCount() const noexcept { return pendingSealCount_; }
    /// Oldest pending seal (pinned; the caller submits its logFd()).
    [[nodiscard]] LogSegment* frontPendingSeal() const noexcept {
        return pendingSealCount_ ? pendingSeals_[0] : nullptr;
    }
    void popPendingSeal() noexcept;
    /// The active segment, for an async flush: pin it, submit its logFd(), and
    /// unpinSegmentFlush() on the completion (M1: its fd outlives the flush).
    [[nodiscard]] LogSegment* activeSegmentForFlush() noexcept { return segments_.rbegin()->second.get(); }
    /// M1: release one in-flight flush on `seg`; a segment already removed from
    /// the log (truncate / retention / reload) is closed when this reaches 0.
    void unpinSegmentFlush(LogSegment* seg) noexcept;
    [[nodiscard]] std::size_t retiredSegmentsForTest() const noexcept { return retiredSegments_.size(); }
    /// Pending (un-submitted) async seals dropped because their segment was
    /// removed (truncate / retention / reload).  The owning worker fails its
    /// seal-gated durable entries retriably when this changes.
    [[nodiscard]] uint64_t droppedPendingSeals() const noexcept { return droppedPendingSeals_; }
    /// Accepted role-change sync work (not on the steady-state loop): the index
    /// msync + directory fsync of LogSegment::recover (run by truncateTo on a
    /// role / epoch change, and at startup recovery).
    [[nodiscard]] uint64_t roleChangeSyncCallsForTest() const noexcept { return storage_.roleChangeSyncs; }

    /// Owner-defined O(1) tag (the owning worker's flush-slot index); UINT32_MAX = none.
    void     setOwnerTag(uint32_t t) noexcept { ownerTag_ = t; }
    [[nodiscard]] uint32_t ownerTag() const noexcept { return ownerTag_; }
    /// Truncate all records with baseOffset >= truncateOffset.
    /// The log's logEndOffset is reset to truncateOffset.
    /// Segments entirely beyond truncateOffset are deleted.
    /// The active segment is physically truncated at the correct byte boundary.
    void truncateTo(int64_t truncateOffset);
    std::vector<int64_t> segmentBaseOffsets() const;
    /// F15 S4 (I-C): the base offset of the first segment whose base is
    /// strictly above `offset`, or -1 if none.  Non-allocating, O(log segments);
    /// owner (append) thread only.  The read_committed build skips an empty
    /// view (a gap below its target) with it.
    [[nodiscard]] int64_t nextSegmentBaseAfter(int64_t offset) const noexcept;
    /// F15 S4 fix round 1: the segment whose base is exactly `segmentBase` —
    /// its written bytes as an untouched read-only mapping (see
    /// LogSegment::mappedBytes), its .log fd (async read-ahead hints), and its
    /// install generation — a Log-wide monotonic identity that changes when the
    /// segment object is replaced (a compaction swap) and is never reused (N1).
    /// {} / gen 0 when absent.  Owner (append) thread only.
    struct SegmentBytes {
        const uint8_t* data = nullptr;
        std::size_t    size = 0;
        uint64_t       gen  = 0;
        int            fd   = -1;
    };
    [[nodiscard]] SegmentBytes segmentBytesAt(int64_t segmentBase);
    int64_t totalBytes() const;
    int deleteOldSegments(int64_t retainBytes);
    /// Directory that contains this log's segment files.
    const std::string& dir() const noexcept { return dir_; }
    [[nodiscard]] bool isDiskBacked() const noexcept { return true; }
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

    // -----------------------------------------------------------------------
    // Background segment pre-provisioning (append-p99 tail fix).
    //
    // The next-segment CREATION (open + ftruncate + mmap of the .index) is the
    // 100% cause of the append-p99 tail: on APFS a single open() is ~35µs p50 /
    // 62µs p99 with multi-ms spikes, so a roll that creates the next segment on
    // the append thread produces a ~200-460µs tail.  We move that creation to a
    // BACKGROUND thread: it pre-builds ONE staging segment (files opened, index
    // ftruncated + mmap'd + prefaulted) under a temporary name.  At roll the
    // append thread ADOPTS it with ZERO syscalls — a pure in-memory rebase (the
    // already-open fd + index mmap are name-agnostic).  The staging → <base>.{log,
    // index} disk rename is DEFERRED to the background thread (finalizeStaging-
    // Renames), off the hot path.  The seal fsync of the OUTGOING segment stays
    // fully synchronous (durability unchanged).
    //
    // Crash-safety: a staging.log adopted-but-not-yet-renamed at crash time holds
    // real data; Log construction recovers its true base from the first batch
    // header and renames it into place (recoverStagingFiles) — no data loss.
    //
    // All cross-thread handoff is wait-free release/acquire.  The APPEND thread is
    // the ONLY mutator of segments_ (single-writer invariant preserved); the
    // BACKGROUND thread only does file I/O and never touches segments_:
    //   preparedSegment_ : bg builds + release-stores; append takes (exchange).
    //   preprovInFlight_ : guards the bg from building two + the fallback race.
    //   renameReq ring   : append → bg  (deferred staging→base disk rename).
    //   renameDone ring  : bg → append  (update logPath_ after the disk rename).
    // -----------------------------------------------------------------------

    /// Background thread: build the next staging segment if none is ready and one
    /// isn't already in flight.  Pure file I/O + object construction on the
    /// caller (background) thread; NEVER touches segments_.  Idempotent and cheap
    /// when a segment is already prepared.  Returns 1 if it built a segment (work
    /// done), 0 otherwise — so it can participate in an agent's doWork idle loop.
    int preprovisionPending();

    /// Background thread: perform any deferred staging→base disk renames enqueued
    /// by the append thread at roll, and publish each completed base back to the
    /// append thread.  NEVER touches segments_.  Returns number of renames done.
    /// Call each background (cleaner) cycle alongside preprovisionPending().
    int finalizeStagingRenames();

    /// Append thread: apply completed staging renames (update in-memory logPath_).
    /// Call each duty cycle from NodeRuntimeAgent::doWork.  Returns count applied.
    int drainStagingRenamesDone();

    /// Right-sized initial .index mmap bytes for a segment holding `rollBytes` of
    /// data at `indexIntervalBytes` sampling.  Was a fixed 8 MiB (64x over-sized);
    /// now ~ (rollBytes/interval + headroom) * 8, clamped to a sane [16 KiB, 8 MiB].
    static int32_t indexMaxBytesFor(int64_t rollBytes, int32_t indexIntervalBytes) noexcept;

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

    /// Return the POSIX fd of the ACTIVE segment's .log file — the fd the
    /// async flusher (IoBackend) must call fdatasync on to make un-synced
    /// appendNoSync() writes durable.  Const, noexcept, no allocation, no
    /// new syscalls — just returns the already-open fd from the active segment.
    /// Keep sync() for the non-async fallback path (unchanged).
    int     activeSegmentFd()  const noexcept { return segments_.rbegin()->second->logFd(); }

    /// The byte offset that becomes durable once the active segment's fd is
    /// fdatasync'd — equal to logEndOffset_.  The async flusher records this
    /// at submit time so it can fire acks for every pending produce whose
    /// lastOffset <= flushBoundary after the completion is reaped.
    int64_t flushBoundary()    const noexcept { return logEndOffset_; }

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
    LogStorageState storage_{};     ///< fail-stop state + inline-sync accounting (shared with segments)
    bool deferSealSync_ = false;
    static constexpr std::size_t kMaxPendingSeals = 16;
    std::array<LogSegment*, kMaxPendingSeals> pendingSeals_{};
    std::size_t pendingSealCount_ = 0;
    uint64_t droppedPendingSeals_ = 0;
    /// M1: segments removed from segments_ while an async flush was in flight on
    /// their fd.  Destroyed (fd closed) by unpinSegmentFlush.  Rare, cold.
    std::vector<std::unique_ptr<LogSegment>> retiredSegments_;
    uint32_t ownerTag_ = UINT32_MAX;
    using SegMap = std::map<int64_t, std::unique_ptr<LogSegment>>;
    /// Wire `seg` into this log (storage state) and store it at `base`, retiring
    /// any pinned segment it replaces.
    void install_(int64_t base, std::unique_ptr<LogSegment> seg);
    /// Remove a segment, retiring it instead of closing its fd if pinned.
    SegMap::iterator erase_(SegMap::iterator it);
    void retire_(std::unique_ptr<LogSegment> seg);
    void throwIfFailed_() const;
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
    /// CP-5.1 (retention-after-restart fix): populated by recover() via a one-time
    /// per-segment batch scan.  Maps baseOffset → real largestTimestamp for every
    /// sealed segment found on disk at recovery time.  publishSealedSnapshot() uses
    /// this map as a fallback when a segment is not present in prevSnap AND
    /// newlySealedMaxTs_ == 0 (i.e. we are on the recover() publish path, not a
    /// hot-path maybeRoll() call).  The map is cleared after the first
    /// publishSealedSnapshot() call consumes it so it does not persist past recovery.
    std::map<int64_t, int64_t> recoveryMaxTimestamps_;
    SegMap segments_;
    uint64_t segInstallGen_ = 0;   ///< F15 S4 N1: segment install generation

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

    // -----------------------------------------------------------------------
    // Background segment pre-provisioning state.
    // -----------------------------------------------------------------------
    /// Right-sized initial .index mmap size for THIS log (derived from rollBytes_
    /// and the default index interval).  Passed to every LogSegment we create.
    int32_t indexMaxBytes_;

    /// Temp-name prefix for pre-provisioned staging segment files.  Files named
    /// <dir>/<kStagingPrefix><seq>.log / .index.  Swept at Log construction if
    /// left over from a crash (they hold no acked data — a crash before the roll
    /// means the segment was never activated).
    static constexpr const char* kStagingPrefix = ".staging-";
    /// Monotonic counter so successive staging files get distinct names (avoids
    /// reusing a name whose fd/mmap the append thread may still hold post-adopt).
    /// Bg-thread-owned (only preprovisionPending writes it).
    std::size_t stagingSeq_ = 0;

    /// Prepared staging segment: bg-thread builds + publishes via release-store;
    /// append-thread takes it (acquire + exchange to nullptr) at roll.  Raw owning
    /// pointer handed across the atomic; whoever sets it to nullptr owns deletion.
    std::atomic<LogSegment*> preparedSegment_{nullptr};
    /// True while a staging segment is prepared OR being built — prevents the bg
    /// thread from building a second one and lets the append thread know a segment
    /// is (or will be) available.  Set by bg thread before building; cleared by
    /// the append thread after it adopts (or discards) the prepared segment.
    std::atomic<bool> preprovInFlight_{false};

    /// Adopt the pre-provisioned staging segment as the active segment at `base`
    /// (in-memory; enqueues the deferred rename), or synchronously create one if
    /// none is ready (append thread only).
    void installNextSegment(int64_t base);
    /// Reconcile staging files left by a crash (append thread, constructor only).
    void recoverStagingFiles();

    // Staging path helpers (naming convention shared by build/rename/recovery).
    std::string stagingLogPathFor(int64_t seq) const;
    std::string stagingIndexPathFor(int64_t seq) const;
    std::string logPathForBase(int64_t base) const;
    std::string indexPathForBase(int64_t base) const;
    /// Rename staging .log/.index → <base>.{log,index} (bg thread / fallback).
    void renameStagingToBase(int64_t seq, int64_t base) noexcept;
    /// Enqueue a deferred staging→base rename (append thread → bg).  Returns false
    /// if the ring is momentarily full (caller renames synchronously as fallback).
    bool enqueueStagingRename(int64_t base, int64_t seq) noexcept;

    // Deferred-rename SPSC rings (rolls are rare → tiny capacity suffices).
    struct RenameReq { int64_t base; int64_t seq; };
    static constexpr std::size_t kRenameRingCapacity = 64; // power of two
    static constexpr std::size_t kRenameRingMask     = kRenameRingCapacity - 1;
    std::array<RenameReq, kRenameRingCapacity> renameReqRing_{};
    alignas(64) std::atomic<std::size_t> renameReqHead_{0}; // bg (consumer)
    alignas(64) std::atomic<std::size_t> renameReqTail_{0}; // append (producer)
    std::array<int64_t, kRenameRingCapacity> renameDoneRing_{};
    alignas(64) std::atomic<std::size_t> renameDoneHead_{0}; // append (consumer)
    alignas(64) std::atomic<std::size_t> renameDoneTail_{0}; // bg (producer)
};
} // namespace rune::storage
