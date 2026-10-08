#pragma once
#include <rune/storage/LogSegment.h>
#include <rune/consensus/ControlRing.h>
#include <rune/storage/ProvisionChannel.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <compare>
#include <cstdint>
#include <iterator>
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
    /// The segment's content lineage (LogSegment::tsLineage) when this entry
    /// was published: a queued cleaner swap / retention proposal carries it and
    /// the owner applies it only to the SAME incarnation.
    uint64_t lineage = 0;
    // E1d (hotpath-A F4): whether the cleaner may compact this segment (it ends
    // at or below the PERSISTED recovery point) is NOT stored per entry any
    // more — the persisted recovery point moves about once per roll and would
    // force a republish of every entry.  The cleaner asks the log:
    // Log::cleanerCompactionAllowedThrough(entry.endOffset).
    friend bool operator==(const SegInfo&, const SegInfo&) = default;
};

class SealedSegmentSnapshot;

/// hotpath-A F4 (backlog D16): sealed-segment snapshots are a PERSISTENT,
/// CHUNKED structure — publishing one never copies the entry list.
///
///  * Entries live in fixed-size chunks (kSnapChunkEntries SegInfo each).  A
///    chunk is SHARED by every directory (and so every snapshot) that covers
///    it; the owner writes a slot in place ONLY when no published snapshot
///    covers it (at or beyond every published end), and never rewrites a
///    published slot.  A change to published entries (compaction install,
///    truncation, reload) copies-on-write the chunk holding the first changed
///    entry and builds the rest of the TAIL fresh — the prefix stays shared.
///  * A directory (SealedSnapshotDir) maps chunk numbers to chunks and holds a
///    fixed pool of snapshot HEADERS: a publish fills the next header and hands
///    it out through an ALIASING shared_ptr of the directory — no allocation.
///    A new directory (O(segments / kSnapChunkEntries) pointer copies) is
///    built only when its chunk slots or its header pool run out.
///
/// So a roll appends ONE entry, a retention drop advances the front (both O(1),
/// allocation-free except one chunk per kSnapChunkEntries rolls and one
/// directory per kSnapHeadersPerDir publishes); truncation / compaction /
/// reload rebuild only the tail from the first changed entry.
inline constexpr std::size_t kSnapChunkEntries  = 64;   // review m3: ~2.5 KiB per chunk
inline constexpr std::size_t kSnapHeadersPerDir = 64;   // review m3: ~1.5 KiB pool per directory
struct SealedSnapshotChunk {
    SegInfo e[kSnapChunkEntries];
};

/// Immutable view of the sealed segments (all but the active one), ascending
/// by base offset.  Vector-like read API (size / [] / random-access iteration
/// / front / back).  Valid while the shared_ptr it was obtained through lives.
class SealedSegmentSnapshot {
public:
    using value_type = SegInfo;
    using size_type  = std::size_t;
    class const_iterator {
    public:
        using iterator_category = std::random_access_iterator_tag;
        using value_type        = SegInfo;
        using difference_type   = std::ptrdiff_t;
        using pointer           = const SegInfo*;
        using reference         = const SegInfo&;
        const_iterator() = default;
        const_iterator(const SealedSegmentSnapshot* s, std::size_t i) noexcept : s_(s), i_(i) {}
        reference operator*() const noexcept { return (*s_)[i_]; }
        pointer operator->() const noexcept { return &(*s_)[i_]; }
        reference operator[](difference_type d) const noexcept { return (*s_)[i_ + static_cast<std::size_t>(d)]; }
        const_iterator& operator++() noexcept { ++i_; return *this; }
        const_iterator operator++(int) noexcept { auto t = *this; ++i_; return t; }
        const_iterator& operator--() noexcept { --i_; return *this; }
        const_iterator operator--(int) noexcept { auto t = *this; --i_; return t; }
        const_iterator& operator+=(difference_type d) noexcept { i_ += static_cast<std::size_t>(d); return *this; }
        const_iterator& operator-=(difference_type d) noexcept { i_ -= static_cast<std::size_t>(d); return *this; }
        friend const_iterator operator+(const_iterator a, difference_type d) noexcept { a += d; return a; }
        friend const_iterator operator+(difference_type d, const_iterator a) noexcept { a += d; return a; }
        friend const_iterator operator-(const_iterator a, difference_type d) noexcept { a -= d; return a; }
        friend difference_type operator-(const_iterator a, const_iterator b) noexcept {
            return static_cast<difference_type>(a.i_) - static_cast<difference_type>(b.i_);
        }
        friend bool operator==(const_iterator a, const_iterator b) noexcept { return a.i_ == b.i_; }
        friend auto operator<=>(const_iterator a, const_iterator b) noexcept { return a.i_ <=> b.i_; }
    private:
        const SealedSegmentSnapshot* s_ = nullptr;
        std::size_t                  i_ = 0;
    };
    using iterator = const_iterator;

    SealedSegmentSnapshot() = default;
    SealedSegmentSnapshot(const struct SealedSnapshotDir* dir, std::size_t first, std::size_t n) noexcept
        : dir_(dir), first_(first), n_(n) {}
    [[nodiscard]] std::size_t size()  const noexcept { return n_; }
    [[nodiscard]] bool        empty() const noexcept { return n_ == 0; }
    [[nodiscard]] const_iterator begin() const noexcept { return {this, 0}; }
    [[nodiscard]] const_iterator end()   const noexcept { return {this, n_}; }
    [[nodiscard]] inline const SegInfo& operator[](std::size_t i) const noexcept;
    [[nodiscard]] const SegInfo& front() const noexcept { return (*this)[0]; }
    [[nodiscard]] const SegInfo& back()  const noexcept { return (*this)[n_ - 1]; }
private:
    const struct SealedSnapshotDir* dir_ = nullptr;   ///< kept alive by the aliasing shared_ptr
    std::size_t first_ = 0;   ///< absolute entry index in dir_
    std::size_t n_     = 0;
};

/// See kSnapChunkEntries.  Entry `a` (absolute) is chunks[a / K]->e[a % K].
struct SealedSnapshotDir {
    std::unique_ptr<std::shared_ptr<SealedSnapshotChunk>[]> chunks;
    std::size_t chunkCap = 0;
    std::unique_ptr<SealedSegmentSnapshot[]> headers;   ///< aliasing-publish targets
    std::size_t headerCap = 0;
};

inline const SegInfo& SealedSegmentSnapshot::operator[](std::size_t i) const noexcept {
    const std::size_t a = first_ + i;
    return dir_->chunks[a / kSnapChunkEntries]->e[a % kSnapChunkEntries];
}

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

    /// Kafka parity (leader stamp): the partitionLeaderEpoch every
    /// appendNoSyncRestamp() writes into the batch header (zero-copy, CRC-
    /// excluded field).  Set by the owning PartitionReplica on becomeLeader,
    /// -1 (leave the producer's bytes) while not leading.  Owner thread only.
    void setLeaderEpochStamp(int32_t epoch) noexcept { leaderEpochStamp_ = epoch; }
    [[nodiscard]] int32_t leaderEpochStamp() const noexcept { return leaderEpochStamp_; }

    std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes);

    /// Zero-copy read: locate the segment containing startOffset and return a
    /// span into its read-only mmap covering whole record batches (see
    /// LogSegment::readView / LogReadView).  No heap allocation, no copy.  Returns
    /// an empty view if startOffset is below logStartOffset() or at/after the LEO.
    /// noexcept (F3): a failed lazy map on the target segment returns an empty
    /// view rather than throwing — see LogSegment::readView.
    LogReadView readView(int64_t startOffset, int32_t maxBytes) noexcept;
    /// #65: LogSegment::readInto on the segment holding startOffset (pread into
    /// `buf`; never counted as a readView, never maps).
    LogReadView readInto(int64_t startOffset, int32_t maxBytes, uint8_t* buf, std::size_t bufCap,
                         bool& firstTooLarge) noexcept;

    int64_t logEndOffset() const noexcept { return logEndOffset_; }
    /// F27 R2: Log-wide segment install generation — changes whenever ANY segment
    /// object is (re)installed (roll, compaction swap, reload, truncation).  A
    /// cached walk over read bytes is valid only while it is unchanged.
    [[nodiscard]] uint64_t installGeneration() const noexcept { return segInstallGen_; }
    /// Bumped by every truncateTo() that removed data: a state derived from
    /// this log's existing content (a producer state) is stale once it moves.
    /// For a log with no replica (passthrough) this is its only generation —
    /// replicated logs use PartitionReplica::logGeneration().  Owner thread.
    [[nodiscard]] uint64_t contentGeneration() const noexcept { return truncations_; }
    /// The lowest offset still readable from this log — the base offset of the
    /// first (oldest) on-disk segment.  Advances past 0 when retention/compaction
    /// deletes the earliest segment(s).  An "Earliest" subscriber MUST start here,
    /// NOT at 0: reading below the first segment base returns empty forever
    /// (Log::read's upper_bound lands on begin() and yields {}), which silently
    /// starves the subscriber (0 delivered).  Returns 0 for an empty log.
    int64_t logStartOffset() const noexcept;
    int64_t recover();
    /// True once recover() has run on this Log object (the LEO is then the
    /// true log end, not just the last segment's base).
    [[nodiscard]] bool isRecovered() const noexcept { return recovered_; }
    /// Flush + fdatasync the active segment's .log and msync its .index.
    /// append() already calls this (durable-by-default). External group-commit
    /// callers may batch appends and call sync() once — but MUST NOT advance
    /// the high-watermark past an offset before sync() returns for that offset.
    void sync();
    /// Test hook: number of inline LogSegment::sync calls on this log's segments
    /// (append's durable fsync, sync(), the inline seal) — spec §3.1: a shard
    /// loop must make none; its flushes go through the io backend.
    [[nodiscard]] uint64_t syncCallsForTest() const noexcept { return storage_.syncCalls; }
    /// readView() calls on this log (single-owner counter; F18 B-2 proves a
    /// fetch costs exactly one lookup and a conflated duplicate none).
    [[nodiscard]] uint64_t readViewCallsForTest() const noexcept { return readViewCalls_; }
    /// Hot-path B F5: on-demand read-map creations / remaps of the ACTIVE segment.
    [[nodiscard]] uint64_t activeReadMapRemapsForTest() const noexcept {
        return segments_.empty() ? 0 : segments_.rbegin()->second->readMapRemaps();
    }
    /// E2 crash-simulation seam: {segment .log path, bytes made durable by the
    /// last inline sync in this incarnation (-1 = none)} for every installed
    /// segment.  A DR harness truncates each file to its durable extent to
    /// simulate a power loss (page cache discarded).  Cold, test-only.
    struct SyncedExtent { std::string logPath; int64_t syncedBytes = -1; };
    [[nodiscard]] std::vector<SyncedExtent> syncedExtentsForTest() const;
    /// Test hook: while non-zero every inline sync fails with this errno.
    void setSyncFaultForTest(int err) noexcept { storage_.faultForTest = err; }
    /// Test hook: while non-zero every data write fails with this errno (E1).
    void setWriteFaultForTest(int err) noexcept { storage_.writeFaultForTest = err; }

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

    // ---- hotpath-A: a roll never blocks the owner (F2 / F3) -----------------
    /// The active segment is full but the roll cannot happen NOW without
    /// blocking the owner: no pre-provisioned segment is ready (provisioned
    /// logs never create one inline — F2) or the async seal queue is full
    /// (worker-driven logs never fsync a seal inline — F3).  O(1), owner thread.
    /// The intake back-pressures on it (Worker: controlled-poll ABORT of the
    /// frame); an append that still arrives (follower apply, txn marker, op
    /// ring) lands in the active segment and the roll happens at the first
    /// append after the condition clears (the segment overshoots rollBytes).
    [[nodiscard]] bool rollDeferred() noexcept {
        if (segments_.empty() || segments_.rbegin()->second->sizeBytes() < rollBytes_) [[likely]] return false;
        return rollBlockedNow_();
    }
    /// hotpath-A F4 (tests): snapshot tail rebuilds / entries they wrote, and
    /// whether the published snapshot equals the sealed segments exactly.
    [[nodiscard]] uint64_t snapshotRebuildsForTest() const noexcept { return snapRebuilds_; }
    [[nodiscard]] uint64_t snapshotRebuiltEntriesForTest() const noexcept { return snapRebuiltEntries_; }
    [[nodiscard]] bool snapshotMatchesSegmentsForTest() const;
    /// F1 (tests): block reservations (fallocate) the OWNER made for a next
    /// segment at roll — 0 for a provisioned log (the provisioner reserves).
    [[nodiscard]] uint64_t ownerPreallocsForTest() const noexcept { return ownerPreallocs_; }
    /// Deferred rolls forced past the overshoot cap (blocking, correct).
    [[nodiscard]] uint64_t forcedRollsForTest() const noexcept { return forcedRolls_; }
    /// A deferred roll is forced once the active segment reaches this size.
    [[nodiscard]] int64_t maxDeferredRollBytesForTest() const noexcept { return maxDeferredRollBytes_(); }
    [[nodiscard]] int64_t segmentSizeForTest(int64_t base) const noexcept {
        auto it = segments_.find(base);
        return it == segments_.end() ? -1 : it->second->sizeBytes();
    }
    /// Rolls deferred by rollDeferred()'s condition (appends that overshot).
    [[nodiscard]] uint64_t rollDeferralsForTest() const noexcept { return rollDeferrals_; }
    static constexpr std::size_t kMaxPendingSealsForTest() noexcept { return kMaxPendingSeals; }
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
    /// F3 round 7: make reloadSegments' pre-install allocation step throw
    /// std::bad_alloc (tests the strong guarantee of the no-throw install).
    void setReloadAllocFaultForTest(bool on) noexcept { reloadAllocFaultForTest_ = on; }
    /// Pending (un-submitted) async seals dropped because their segment was
    /// removed (truncate / retention / reload).  The owning worker fails its
    /// seal-gated durable entries retriably when this changes.
    [[nodiscard]] uint64_t droppedPendingSeals() const noexcept { return droppedPendingSeals_; }
    /// F30 fix round 1: the lowest LEO this log was truncated to since the
    /// owner last took it (kNoTruncation = none), then reset.  The owning
    /// worker clamps its synced-LEO bookkeeping to it, so bytes re-appended
    /// after a truncation are never treated as already flushed.  Owner-thread
    /// only (the thread that appends / truncates); no allocation.
    static constexpr int64_t kNoTruncation = INT64_MAX;
    /// Largest accepted segment roll size: segment positions are 32-bit, and a
    /// deferred roll may overshoot rollBytes by one maximal batch (64 MiB
    /// headroom).  NodeConfig refuses a larger segmentRollBytes (D19 L1).
    static constexpr int64_t kMaxRollBytes = int64_t{INT32_MAX} - (int64_t{64} << 20);
    [[nodiscard]] int64_t takeTruncationLowWater() noexcept {
        const int64_t v = truncationLowWater_;
        truncationLowWater_ = kNoTruncation;
        return v;
    }
    /// Accepted role-change sync work (not on the steady-state loop): the index
    /// msync + directory fsync of LogSegment::recover (run by truncateTo on a
    /// role / epoch change, and at startup recovery).
    [[nodiscard]] uint64_t roleChangeSyncCallsForTest() const noexcept { return storage_.roleChangeSyncs; }

    // ---- E1d: durable recovery point (Kafka recovery-point parity) -----------
    //
    // R = every offset below it is known durable on disk.  Advanced IN ORDER on
    // the owner thread: a sealed segment counts only once its seal fdatasync
    // completed (inline, or the io backend's completion -> noteFlushCompleted),
    // the active segment up to its synced extent.
    //
    // Persisted in <dir>/recovery-point-offset-checkpoint: a PRE-ALLOCATED file
    // of two independent slots (one per 4 KiB block), each
    // {magic, version, seq, R, crc32c}.  The file is created (tmp + fsync +
    // rename + FULL directory fsync) ONCE, at the first recovery; afterwards a
    // checkpoint only overwrites the ALTERNATE slot in place + fdatasync — no
    // create, no rename, no directory sync.  Recovery takes the valid slot
    // with the highest seq (a torn slot = the older, lower, safe R); a file
    // whose two slots are both invalid is fail-stop (KafkaStorageException).
    //
    // A checkpoint is due when R crossed a segment boundary since the last
    // persist (≈ once per roll — never per append).  Worker-driven logs
    // (deferSealSync) checkpoint through the io backend (one write+fdatasync
    // request: no syscall on the loop); every other path (legacy inline seal,
    // truncation, recovery, clean shutdown) writes inline.  A stale (lower)
    // persisted R is always safe.
    static constexpr const char* kRecoveryPointFile    = "recovery-point-offset-checkpoint";
    static constexpr const char* kRecoveryPointTmpFile = "recovery-point-offset-checkpoint.tmp";
    static constexpr int64_t  kRecoveryPointUnknown = -1;
    static constexpr uint32_t kRpMagic        = 0x31505252u;   // "RRP1" (LE)
    static constexpr uint16_t kRpVersion      = 1;
    static constexpr std::size_t kRpSlotBytes  = 32;           ///< one slot image
    static constexpr std::size_t kRpSlotStride = 4096;         ///< one slot per block
    static constexpr std::size_t kRpFileBytes  = 2 * kRpSlotStride;
    /// Encode / decode one slot image (pure; tests use them to tear / forge slots).
    static void encodeRpSlot(uint8_t (&out)[kRpSlotBytes], uint64_t seq, int64_t r) noexcept;
    /// true + seq/r for a valid slot (magic, version, crc32c, r >= 0).
    static bool decodeRpSlot(const uint8_t* in, uint64_t& seq, int64_t& r) noexcept;
    enum class RpFileState : uint8_t { Missing, Valid, Corrupt };
    struct RpFileRead { RpFileState state = RpFileState::Missing; int64_t r = kRecoveryPointUnknown;
                        uint64_t seq = 0; int slot = -1; };
    /// Read <dir>/recovery-point-offset-checkpoint (cold).
    static RpFileRead readRecoveryPointFile(const std::string& dir) noexcept;

    /// In-memory durable recovery point (owner thread).
    [[nodiscard]] int64_t recoveryPoint() const noexcept { return recoveryPoint_; }
    /// The value restart recovery would read (kRecoveryPointUnknown = none).
    /// Moves only once a checkpoint is DURABLE (never at write submit).
    [[nodiscard]] int64_t persistedRecoveryPoint() const noexcept { return persistedRp_; }
    /// Upper bound of the R restart recovery may read: the confirmed value or
    /// any possibly-landed (unconfirmed) checkpoint write above it (MAJOR-1).
    [[nodiscard]] int64_t onDiskRecoveryPointUpperBound() const noexcept {
        return rpUnconfirmedMax_ > persistedRp_ ? rpUnconfirmedMax_ : persistedRp_;
    }
    /// True iff restart recovery trusts a sealed segment ending at `segEnd`
    /// as-is — the cleaner may compact it (E1d invariant 3).
    [[nodiscard]] bool compactionAllowedThrough(int64_t segEnd) const noexcept {
        return persistedRp_ == kRecoveryPointUnknown || segEnd <= persistedRp_;
    }
    /// Cleaner thread (hotpath-A F4): compactionAllowedThrough() against the
    /// persisted recovery point the owner PUBLISHED (release-store whenever it
    /// moves — no snapshot republish).  Advisory: the owner re-checks
    /// compactionAllowedThrough() before installing any compaction output.
    [[nodiscard]] bool cleanerCompactionAllowedThrough(int64_t segEnd) const noexcept {
        const int64_t rp = cleanerPersistedRp_.load(std::memory_order_acquire);
        return rp == kRecoveryPointUnknown || segEnd <= rp;
    }
    /// An async (io backend) fdatasync of `seg`'s fd completed successfully:
    /// every byte written to it before the submit — offsets below `upTo` (the
    /// LEO at submit; INT64_MAX for a seal = the whole segment) — is durable.
    /// Advances R in order.  Returns true iff a checkpoint is now due (the
    /// owner queues the log — event-driven, F8).  O(log segments), no allocation.
    bool noteFlushCompleted(LogSegment* seg, int64_t upTo) noexcept;
    /// Persist R now, inline (cold paths).  0 or the errno (fail-stop).
    int checkpointRecoveryPoint() noexcept;
    /// Hot-path B F7 (worker-driven logs, deferSealSync): E1d F6 requires the
    /// on-disk recovery point <= the truncation's resulting LEO before
    /// truncateTo touches any file.  Instead of truncateTo's inline write +
    /// fdatasync on the worker, the owner's io backend writes the lowered value
    /// first: true = already durable (truncateTo writes nothing inline); false
    /// = a lowering checkpoint is due / in flight (takeCheckpointKick() queues
    /// it on the owner) — retry next pass (FollowerSync TruncatePending).  Once
    /// armed, R never advances above the cut until the truncation ran or was
    /// abandoned (cancelColdPathWait).  Inline-sync logs: always true.
    bool prepareTruncationTo(int64_t x) noexcept;
    /// Owner: a lowering checkpoint became due outside a completion (once).
    [[nodiscard]] bool takeCheckpointKick() noexcept { const bool k = rpKick_; rpKick_ = false; return k; }
    /// Review C1 (Kafka: the leader-epoch checkpoint is flushed before the
    /// recovery point advances): the owner's replica caps R at the lowest
    /// offset whose epoch history is not yet durable (INT64_MAX = none).
    /// Raising it may let R advance (a checkpoint kick follows).  Owner thread.
    void setEpochHistoryCap(int64_t c) noexcept;
    /// Review C1: before recover() — recovery keeps the persisted recovery
    /// point at the startup value (never raises it to the recovered LEO) until
    /// the epoch-history owner lifts the cap (setEpochHistoryCap).  Automatic
    /// for a directory that holds a replica's epoch history
    /// (leader-epoch-state / leader-epoch-checkpoint): its PartitionReplica
    /// lifts it once the reconciled history is durable.
    void setEpochHistoryGuard(bool on) noexcept { epochGuard_ = on; }
    /// The recovery point recovery started from (persisted value, or the
    /// upgrade default) — the replica re-derives epoch history from here.
    /// kRecoveryPointUnknown before recover().
    [[nodiscard]] int64_t startupRecoveryPoint() const noexcept { return startupRp_; }
    [[nodiscard]] int64_t epochHistoryCapForTest() const noexcept { return rpEpochCap_; }
    [[nodiscard]] uint64_t asyncTruncationLoweringsForTest() const noexcept { return rpLowerings_; }
    [[nodiscard]] uint64_t inPlaceTruncationsForTest() const noexcept { return inPlaceTruncations_; }
    /// Review Md5 — provisioner thread: hand a read mapping built for this
    /// log's outstanding growth request back to the owner.  false = ring full
    /// (the caller destroys it; the owner re-requests).
    [[nodiscard]] bool offerReadMap(MappedFile* m, int64_t packedRequest) noexcept {
        // Re-review N2: the answer carries the request's identity (its seq).
        return mapRing_.offer(kMapGrownMsg, static_cast<int64_t>(reinterpret_cast<std::uintptr_t>(m)),
                              requestSeq(packedRequest));
    }
    [[nodiscard]] bool offerReadMapFailure(int err, int64_t packedRequest) noexcept {
        return mapRing_.offer(kMapFailedMsg, (static_cast<int64_t>(err) << 32) | requestSeq(packedRequest), 0);
    }
    /// Re-review N2: a growth request packs (dup'd fd : 24 | MiB : 20 | seq : 19).
    [[nodiscard]] static int64_t packRequest(int fd, int64_t mib, int64_t seq) noexcept {
        return (static_cast<int64_t>(fd) & 0xFFFFFF) | ((mib & 0xFFFFF) << 24) | ((seq & 0x7FFFF) << 44);
    }
    [[nodiscard]] static int requestFd(int64_t p) noexcept { return static_cast<int>(p & 0xFFFFFF); }
    [[nodiscard]] static int64_t requestMiB(int64_t p) noexcept { return (p >> 24) & 0xFFFFF; }
    [[nodiscard]] static int64_t requestSeq(int64_t p) noexcept { return (p >> 44) & 0x7FFFF; }
    [[nodiscard]] uint64_t readMapsDiscardedForTest() const noexcept { return readMapsDiscarded_; }
    /// D30: read mappings retired when their segment sealed.
    [[nodiscard]] uint64_t readMapsRetiredAtSealForTest() const noexcept { return readMapsRetiredAtSeal_; }
    /// D30: retire every lazily mapped sealed segment (a finished walk of old data).
    void releaseLazySealedMaps() noexcept;
    [[nodiscard]] std::size_t lazySealedMappedForTest() const noexcept { return mappedSealedCount_; }
    [[nodiscard]] uint64_t sealedLazyRemapsForTest() const noexcept { return sealedLazyRemaps_; }
    [[nodiscard]] uint64_t sealedMapsEvictedForTest() const noexcept { return sealedMapsEvicted_; }
    [[nodiscard]] uint64_t readMapsDisposedInlineForTest() const noexcept { return readMapsDisposedInline_; }
    [[nodiscard]] static constexpr std::size_t mappedSealedCapForTest() noexcept { return kMappedSealedCap; }
    [[nodiscard]] uint64_t ownerFallbackRemapsForTest() const noexcept { return ownerFallbackRemaps_; }
    /// Tests (re-review N2): force the count-based re-request on the next need.
    void expireReadMapRequestForTest() noexcept { growWaits_ = 1u << 30; }
    [[nodiscard]] uint64_t readMapGrowthsForTest() const noexcept { return readMapGrowths_; }
    [[nodiscard]] uint64_t readMapGrowthFailuresForTest() const noexcept { return readMapGrowthFailures_; }
    /// Cleaner-thread fsyncs / directory fsyncs (retention log-start
    /// checkpoints, posted renames).  Read only after the cleaner stopped.
    [[nodiscard]] uint64_t cleanerFsyncsForTest() const noexcept { return cleanerFsyncs_; }
    [[nodiscard]] uint64_t cleanerDirFsyncsForTest() const noexcept { return cleanerDirFsyncs_; }
    /// Directory mutations (create / rename / unlink) since construction.
    [[nodiscard]] uint64_t directoryMutationsForTest() const noexcept { return dirMutGen_; }

    // Async checkpoint (owning worker, through its io backend):
    //   Idle --prepare--> Prepared --submitted--> InFlight --completed--> Idle
    //   (Prepared --notSubmitted--> Idle on back-pressure; due stays set.)
    enum class RpStage : uint8_t { Idle, Prepared, InFlight };
    [[nodiscard]] RpStage recoveryCheckpointStage() const noexcept { return rp_.stage; }
    /// R crossed a segment boundary since the last persist and no checkpoint is
    /// in progress (O(1)).
    [[nodiscard]] bool recoveryCheckpointDue() const noexcept {
        return (rpDue_ || rpLowerDue_) && rp_.stage == RpStage::Idle && recovered_ && rpFd_ >= 0 &&
               storage_.failedErrno == 0;
    }
    /// The write the owner submits: `len` bytes of `data` at `offset` of `fd`,
    /// then fdatasync(fd).  `data` stays valid until the next prepare.
    struct RpWrite { int fd = -1; const uint8_t* data = nullptr; uint32_t len = 0; int64_t offset = 0; uint64_t seq = 0; };
    /// Idle -> Prepared: encode the alternate slot.  False when not due.
    /// Pure memory (no syscall).
    bool prepareRecoveryCheckpoint(RpWrite& out) noexcept;
    /// Prepared -> InFlight (the write+fdatasync was submitted).
    void recoveryCheckpointSubmitted() noexcept;
    /// Prepared -> Idle (submit refused: back-pressure; still due).
    void recoveryCheckpointNotSubmitted() noexcept;
    /// InFlight completed (err = its errno, 0 = ok).  Success: the slot is
    /// durable -> persistedRecoveryPoint() moves (and the cleaner gate).  A
    /// superseded (abandoned) checkpoint never moves it.  An error is
    /// fail-stop.  Returns true iff another checkpoint is due.
    bool recoveryCheckpointCompleted(uint64_t seq, int err) noexcept;
    /// F7: a fresh owner took this log and holds no route for an in-flight
    /// checkpoint (its completion went to a previous owner, unobserved): the
    /// outcome is unknown — the slot is treated as unwritten (the next write
    /// targets it again with a higher seq) and R is re-checkpointed.
    void resetRecoveryCheckpointForNewOwner() noexcept;
    /// F7: re-queue (pinned) a seal for every sealed segment not yet durable,
    /// not already pending and not in `inFlight[0, nInFlight)` (segments whose
    /// seal is routed on the new owner — MEDIUM-1: the replay runs even when
    /// another seal of this log is in flight) — a fresh owner's replay of seal
    /// completions a previous owner never observed.  Cold.  Returns the number queued.
    std::size_t requeueUndurableSeals(const LogSegment* const* inFlight = nullptr,
                                      std::size_t nInFlight = 0) noexcept;
    // ---- E1d MEDIUM-2: directory durability (Kafka flushParentDir parity) ----
    // R never passes the base of a segment whose directory entry (create /
    // adopt-rename, or a truncation's deletions) is not yet durable: recovery
    // trusts segments below R WITHOUT a chain check, so a lost entry there
    // would read as a compaction gap (a silent offset hole).  Every directory
    // mutation bumps a generation; a segment records the generation of its
    // entry; a directory fsync submitted at generation G makes every entry
    // <= G durable.  Worker-driven logs batch ONE directory fsync through the
    // io backend (never on the loop) when R is blocked by such an entry; the
    // legacy inline-seal path fsyncs the directory inline at the roll.
    //   Idle --prepare--> InFlight(gen) --completed--> Idle
    struct DirSync { int fd = -1; uint64_t gen = 0; };
    /// R is blocked by a not-yet-durable directory entry and no directory
    /// fsync is in flight (O(1)).
    [[nodiscard]] bool directorySyncDue() const noexcept;
    /// Idle -> InFlight: the directory fd + the generation it will cover.
    /// False when not due.  Pure memory.
    bool prepareDirectorySync(DirSync& out) noexcept;
    /// InFlight -> Idle (submit refused: back-pressure; still due).
    void directorySyncNotSubmitted() noexcept;
    /// The directory fsync covering `gen` completed (err = errno, 0 = ok).
    /// Success: entries <= gen are durable, R advances.  Error: fail-stop.
    /// Returns true iff a checkpoint or another directory fsync is now due.
    bool directorySyncCompleted(uint64_t gen, int err) noexcept;
    /// F7 parity: a fresh owner holds no route for an in-flight directory
    /// fsync (its completion went to a previous owner): treated as not done.
    void resetDirectorySyncForNewOwner() noexcept;
    [[nodiscard]] uint64_t directorySyncsCompletedForTest() const noexcept { return dirSyncsDone_; }
    [[nodiscard]] uint64_t recoveryCheckpointsForTest() const noexcept { return rpPersists_; }
    /// Crash seam (DR / storage tests): the next destruction is a CRASH, not a
    /// clean shutdown — no recovery-point checkpoint is written.
    void crashForTest() noexcept { crashedForTest_ = true; }

    /// Owner-defined O(1) tag (the owning worker's flush-slot index); UINT32_MAX = none.
    void     setOwnerTag(uint32_t t) noexcept { ownerTag_ = t; }
    [[nodiscard]] uint32_t ownerTag() const noexcept { return ownerTag_; }
    /// Truncate all records with baseOffset >= truncateOffset.
    /// The log's logEndOffset is reset to truncateOffset.
    /// Segments entirely beyond truncateOffset are deleted.
    /// The active segment is physically truncated at the correct byte boundary.
    /// Fix round 5: returns false (nothing done) when the cleaner has not yet
    /// performed every posted file operation — retry on a later duty pass (the
    /// replica parks in FollowerSync::TruncatePending; never waits).
    [[nodiscard]] bool truncateTo(int64_t truncateOffset);
private:
    bool truncateTo_(int64_t truncateOffset);
public:
    std::vector<int64_t> segmentBaseOffsets() const;
    /// F15 S4 (I-C): the base offset of the first segment whose base is
    /// strictly above `offset`, or -1 if none.  Non-allocating, O(log segments);
    /// owner (append) thread only.  The read_committed build skips an empty
    /// view (a gap below its target) with it.
    [[nodiscard]] int64_t nextSegmentBaseAfter(int64_t offset) const noexcept;
    /// One past the last written offset of the segment containing `offset` (-1 if none).
    [[nodiscard]] int64_t segmentNextOffsetAt(int64_t offset) const noexcept;
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
        /// The segment's written bytes: data == nullptr with written > 0 is an
        /// UNREADABLE segment (its mapping failed) — never "walked" (D30 MAJOR-2).
        std::size_t    written = 0;
    };
    [[nodiscard]] SegmentBytes segmentBytesAt(int64_t segmentBase);
    int64_t totalBytes() const;
    /// Size retention: delete the oldest sealed segments until the log fits in
    /// retainBytes — only segments that END at or below `highWatermark` (Kafka:
    /// HW >= nextSegment.baseOffset), never the active one.
    int deleteOldSegments(int64_t retainBytes, int64_t highWatermark = INT64_MAX);
    /// Directory that contains this log's segment files.
    const std::string& dir() const noexcept { return dir_; }
    [[nodiscard]] bool isDiskBacked() const noexcept { return true; }
    /// Reload all segments from disk (close + reopen every LogSegment).
    /// Used by LogCleaner after atomically rewriting segment files on disk,
    /// so that in-memory file handles reflect the new file contents.
    bool reloadSegments();   ///< false = deferred (see truncateTo)
    /// CP-2: reload a SINGLE sealed segment in place (close + reopen just that
    /// LogSegment) after its .log file was atomically rewritten by the cleaner.
    /// O(1) in the number of segments — avoids the O(n) full-log reloadSegments()
    /// that re-recover()s every segment (which makes per-segment compaction O(n^2)).
    /// If the segment's .log file no longer exists on disk (the cleaner emptied and
    /// deleted it), the segment is dropped from the in-memory map.  MUST NOT be
    /// called for the active segment and does NOT change logEndOffset_ (sealed-
    /// segment rewrites never move the LEO).
    void reloadSegment(int64_t baseOffset);

    /// CP-6/race-fix: the latest immutable snapshot of sealed segments, as the
    /// OWNER (single-writer append thread) last published it — owner thread /
    /// owner-driven tests only.  Sealed-segment sizes are fixed once sealed so
    /// the snapshot is stable.
    std::shared_ptr<const SealedSegmentSnapshot> ownerSealedSnapshot() const noexcept { return ownerSnap_; }

    /// The latest published snapshot for THE cleaner thread (its one consumer):
    /// a latest-value triple buffer (D22) — no lock, no allocation.  The cleaner
    /// MUST use this instead of segmentSize() / segmentBaseOffsets(), which race
    /// with the append thread's mutations to segments_ (UB confirmed by TSan).
    /// (Was std::atomic_load on a shared_ptr: a hidden libstdc++ mutex pool on the
    /// owner's roll.)
    std::shared_ptr<const SealedSegmentSnapshot> cleanerSnapshot() const noexcept {
        if (snapMiddle_.load(std::memory_order_relaxed) & kSnapFresh) {
            const uint8_t prev = snapMiddle_.exchange(snapFront_, std::memory_order_acq_rel);
            snapFront_ = static_cast<uint8_t>(prev & kSnapIndex);
        }
        return snapSlot_[snapFront_];
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
    /// `lineage` = the SegInfo::lineage the cleaner acted on (kAnyLineage skips
    /// the check — tests / legacy callers).  The swap is dropped when the base
    /// is now the ACTIVE segment or carries a different lineage (the cleaner
    /// acted on a stale snapshot: a truncate rebuilt the segment meanwhile).
    static constexpr uint64_t kAnyLineage = 0;
    /// Max members of one deferred compaction group (== LogCleaner::kMaxDeferredGroupMembers).
    static constexpr std::size_t kMaxGroupMembers = 64;
    /// Fix round 2 file naming (shared with LogCleaner):
    ///   <base20>.log.<seq>.cleaned — a cleaner output awaiting owner validation
    ///                                (uncommitted: recovery deletes it);
    ///   <base20>.log.<end>.swap    — a validated swap replacing every segment in
    ///                                [base, end) (committed: recovery completes it).
    static std::string cleanedLogPath(const std::string& dir, int64_t base, uint64_t seq);
    static std::string swapLogPath(const std::string& dir, int64_t base, int64_t end);
    ///   <base20>.index.<seq>.cleaned — the cleaner-built index of that output.
    static std::string cleanedIndexPath(const std::string& dir, int64_t base, uint64_t seq);
    /// One member of a deferred compaction group (cleaner thread → owner thread).
    struct CleanedMember { int64_t base = 0; uint64_t lineage = kAnyLineage; };
    /// Fix round 2 (Kafka-style replaceSegments on the owner thread): enqueue a
    /// cleaner output for validation + install.  ALL members are claimed in the
    /// ring at once (one release-store) so the owner sees the whole group or
    /// nothing.  The owner installs it only if every member still exists with
    /// the same lineage, none is the active segment, they are contiguous and
    /// below the drain bound; otherwise it posts a rejection (the cleaner
    /// deletes the output) and the originals stay untouched.  false = ring full (the caller deletes its
    /// output and retries next pass).
    /// `prebuilt` (fix round 4, ownership transferred on success): the output
    /// opened by openCleanedOutput — the owner switches to it without any
    /// filesystem operation.  nullptr with hasOutput = the owner rejects.
    bool enqueueCleanedGroup(const CleanedMember* members, std::size_t n, uint64_t seq,
                             bool hasOutput, int64_t nextOffset = 0,
                             LogSegment* prebuilt = nullptr) noexcept;
    /// LEGACY / tests only (no production caller since fix round 2): reload
    /// `base` in place from a file a synchronous caller already replaced —
    /// reloadSegment + recover() (re-read, CRC, index rebuild, fsync) on the
    /// owner.  Production compaction uses enqueueCleanedGroup (O(1) install).
    bool enqueueCleanedSegment(int64_t base, uint64_t lineage = kAnyLineage) noexcept;

    /// CP-6: Drain pending cleaner swaps on the single-writer owner thread (once
    /// per duty pass).  A deferred group is validated (metadata) and installed
    /// with NO filesystem operation (fix round 4): the in-memory map switches to
    /// the cleaner-opened handle and the renames / object destruction are posted
    /// to the cleaner (runCleanerFileOps).  AT MOST ONE group per call; the rest
    /// stay queued.  Legacy reload entries are reloaded (reloadSegment).
    ///
    /// Lock-free SPSC: this is the SINGLE-CONSUMER half.  The acquire-load of
    /// tail_ pairs with enqueueSwapSlots_'s release-store.
    /// Returns number of ring slots consumed (>0 = work done).
    /// `maxBaseBound` (fix round 2): a deferred group is installed only when every
    /// member base is below it — the owner passes min(HW, LSO) (the cleaner
    /// selected members strictly below both).  Legacy reload entries ignore it.
    int drainCleanerSwaps(int64_t maxBaseBound = INT64_MAX);
    /// Deferred groups rejected by validation (their output deleted).  Owner thread.
    [[nodiscard]] uint64_t cleanerGroupsRejected() const noexcept { return cleanerGroupsRejected_; }
    /// Cleaner thread: outputs it enqueued are not yet consumed by the owner.
    [[nodiscard]] bool cleanerSwapsPending() const noexcept;
    /// The lowest front base of a deferred group rejected since the last call
    /// (INT64_MAX = none), then reset.  The owner rewinds the replica's clean
    /// watermark to it: the rejected range was NOT compacted although the
    /// cleaner already advanced past it.  Owner thread.
    [[nodiscard]] int64_t takeCleanerRejectLowWater() noexcept {
        const int64_t v = cleanerRejectLowWater_;
        cleanerRejectLowWater_ = INT64_MAX;
        return v;
    }

    // -----------------------------------------------------------------------
    // CP-9: Retention async-delete API (single-writer invariant preserved).
    //
    // The cleaner agent (off the append thread) selects segments eligible for
    // deletion and PROPOSES each (base, lineage) via enqueueDeletedSegment() —
    // it never renames a live file (fix round 3).
    //
    // The owner calls drainRetentionDeletes(hw), which validates each proposal,
    // renames <base>.log/.index → *.deleted and erases the segment from
    // segments_ (the ONLY mutation path, preserving the single-writer invariant).
    // Compaction installs also rename replaced files to *.deleted.
    //
    // Physical removal of *.deleted files is deferred: the cleaner calls
    // discoverDeletedFiles + physicallyRemoveExpiredDeletedFiles(nowMs,
    // fileDeleteDelayMs) once per retention cycle.
    //
    // Crash-safety: Log construction scans for .deleted files left by a prior
    // crash and records them for immediate physical cleanup (they were already
    // absent from segments_ since their .log was renamed before crash, so they
    // cannot be re-opened by a restart).
    // -----------------------------------------------------------------------

    /// CP-9 SPSC producer (cleaner thread): PROPOSE a segment for retention
    /// deletion (fix round 3: nothing on disk changes here — the owner validates
    /// and renames the files in drainRetentionDeletes).
    /// Returns false (drops) if the ring is momentarily full; cleaner retries
    /// next cycle (idempotent — drainRetentionDeletes is a no-op for an unknown
    /// base).
    /// `lineage`: the incarnation the cleaner saw — a proposal for the active
    /// segment or a different lineage is dropped, never erasing live data.
    bool enqueueDeletedSegment(int64_t base, uint64_t lineage = kAnyLineage) noexcept;
    /// Cleaner work dropped because it named the active segment or a stale
    /// lineage (MAJOR-2 observability).  Owner thread.
    [[nodiscard]] uint64_t staleCleanerOpsDropped() const noexcept { return staleCleanerOps_; }

    /// CP-9 SPSC consumer (owner thread): validate retention proposals; for each
    /// valid one rename its files to *.deleted, erase it from segments_ and
    /// update the sealed snapshot.  Called from NodeRuntimeAgent::doWork (same thread as
    /// all segments_ mutations).
    /// Returns number of segments erased from segments_.
    /// `maxEndBound` (fix round 3): a segment is deleted only if it ENDS at or
    /// below it (the owner passes its HW).
    int drainRetentionDeletes(int64_t maxEndBound = INT64_MAX);
    /// Cleaner thread: schedule newly renamed *.deleted files for physical removal.
    void discoverDeletedFiles(int64_t nowMs);
    /// Fix round 4 (cleaner thread, once per cycle per log): perform the file
    /// operations the owner posted (compaction renames, retention renames,
    /// rejected-output deletion), destroy the segment objects it retired, fsync
    /// the directory, and sweep expired *.deleted files.  Never waits: skips
    /// the cycle if the owner holds the file-op token.  Returns operations done.
    int runCleanerFileOps(int64_t nowMs, int64_t fileDeleteDelayMs = 0);
    /// Owner: every posted file operation has been performed by the cleaner
    /// (owner cold paths — truncate / reload / size retention — need it).
    [[nodiscard]] bool fileOpsQuiescent() const noexcept;
    /// Owner: try to start a cold path now (true = quiescent; the caller then
    /// calls truncateTo / reloadSegments).  false = it is deferred and the
    /// owner's drains post nothing until it runs or is cancelled.
    [[nodiscard]] bool tryBeginColdPath() noexcept { return beginColdPath_(INT64_MIN); }
    /// Owner: a deferred cold path was abandoned (role change) — resume posting.
    void cancelColdPathWait() noexcept {
        pauseSwapPost_ = pauseDropPost_ = false;
        clearTruncationCap_();   // hot-path B F7: an abandoned truncation releases R
    }
    /// Cleaner thread: posted operations not yet performed (a compaction pass
    /// must not start on this log before they are — fix round 5).
    [[nodiscard]] bool cleanerFileOpsPending() const noexcept;
    /// Test / observability (cleaner thread).
    void failNextCleanerApplyForTest() noexcept { failNextCleanerApplyForTest_ = true; }
    [[nodiscard]] uint64_t checkpointWriteFailuresForTest() const noexcept { return checkpointWriteFailures_; }
    /// Cleaner thread: open a finished output as a trusted sealed segment
    /// (handed to the owner with enqueueCleanedGroup).  Throws on failure.
    std::unique_ptr<LogSegment> openCleanedOutput(int64_t base, uint64_t seq, int64_t nextOffset) const;

    // -----------------------------------------------------------------------
    // Background segment pre-provisioning (append-p99 tail fix).
    //
    // The next-segment CREATION (open + ftruncate + mmap of the .index) is the
    // 100% cause of the append-p99 tail: on APFS a single open() is ~35µs p50 /
    // 62µs p99 with multi-ms spikes, so a roll that creates the next segment on
    // the append thread produces a ~200-460µs tail.  We move that creation to a
    // BACKGROUND thread: it pre-builds staging segments (files opened, index
    // ftruncated + mmap'd + prefaulted, blocks fallocated) under temporary names.  At roll the
    // append thread ADOPTS it with ZERO syscalls — a pure in-memory rebase (the
    // already-open fd + index mmap are name-agnostic).  The staging → <base>.{log,
    // index} disk rename is DEFERRED to the background thread (finalizeStaging-
    // Renames), off the hot path.  The seal fsync of the OUTGOING segment is
    // queued for the owner's io backend (worker-driven logs) or inline (legacy).
    //
    // Crash-safety: a staging.log adopted-but-not-yet-renamed at crash time holds
    // real data; Log construction recovers its true base from the first batch
    // header and renames it into place (recoverStagingFiles) — no data loss.
    //
    // All cross-thread handoff is wait-free release/acquire.  The APPEND thread is
    // the ONLY mutator of segments_ (single-writer invariant preserved); the
    // BACKGROUND thread only does file I/O and never touches segments_:
    //   ready ring       : bg → append  (built segments; see below).
    //   renameReq ring   : append → bg  (deferred staging→base disk rename).
    //   renameDone ring  : bg → append  (update logPath_ after the disk rename).
    // -----------------------------------------------------------------------

    // hotpath-A (F1 + F2): the ready pool.  The provisioning thread keeps up to
    // `depth` READY segments per log on a ring (rune::consensus::ControlRing —
    // the agrona ring copy; provisioner = producer, owner = the one consumer).
    // Each one is fully built off the owner: files created + opened, index
    // ftruncated + mmapped + prefaulted, read map created AND its blocks
    // fallocated (F1).  A roll adopts the oldest one with zero syscalls.
    //
    // Demand-driven (review M3): a PROVISIONED log reserves nothing while its
    // active segment is below the fill threshold; the owner posts a Threshold
    // demand (ProvisionChannel) when the active segment crosses it, and an Adopted
    // demand after every adoption (the provisioner then finalizes the rename).
    // A provisioned log never creates a segment on the owner while the
    // provisioner can still deliver: with no ready segment the roll is deferred
    // (rollDeferred(); the intake holds).  Every Threshold demand ends in a
    // ready segment or a FAILURE record on the same ring (review M1: a failed
    // build, or the node-wide reservation cap) — on a failure the owner rolls
    // INLINE once (blocking, correct, counted + rate-limited log; a failed
    // inline create fails the log exactly as before).  Crash safety: a ready
    // segment never written is an empty staging file — construction deletes it
    // (recoverStagingFiles); a clean shutdown deletes the unconsumed ones.
    static constexpr std::size_t kMaxProvisionDepth = 8;
    /// Threshold: the active segment crossed the fill threshold (build).
    /// Adopted: a ready segment was adopted (finalize its rename).
    /// Hold: a roll is being HELD for want of a ready segment — posted at the
    /// first blocked check of an episode and again every kHoldRepostChecks
    /// blocked checks (re-review N2: recovers a demand the provisioner could
    /// not keep; counted as the roll-hold metric).  Served like Threshold.
    enum class ProvisionDemand : int64_t { Threshold = 1, Adopted = 2, Hold = 3 };
    static constexpr uint32_t kHoldRepostChecks = 1024;
    /// Owner (before the log is driven): this log is served by a provisioning
    /// thread.  `thresholdPct` (1..100): fill level of the active segment at
    /// which the next segment is demanded.  Legacy (never called): an empty
    /// pool falls back to the inline create (control-thread logs, tests).
    /// Demands, read-map growth requests and retired read maps go to `channel`
    /// (D19 L3: a concrete channel — a direct call on the roll path, never an
    /// indirect one).  A full channel returns false: the owner re-posts later
    /// (never lost).  nullptr: provisioned, but no demand is ever posted (tests).
    void setProvisioning(ProvisionChannel* channel, int thresholdPct = 50) noexcept {
        provisioned_ = true;
        provisionChannel_ = channel;
        asyncReadGrowth_ = channel != nullptr && channel->servesReadMaps();

        const int pct = thresholdPct < 1 ? 1 : (thresholdPct > 100 ? 100 : thresholdPct);
        provisionThresholdBytes_ = rollBytes_ / 100 * pct + (rollBytes_ % 100) * pct / 100;
    }
    [[nodiscard]] bool provisioned() const noexcept { return provisioned_; }
    [[nodiscard]] int64_t rollBytes() const noexcept { return rollBytes_; }
    /// Provisioning thread: top the ready pool up to `depth` (clamped to
    /// [1, kMaxProvisionDepth]), at most `depth` builds per call.  Pure file I/O
    /// + object construction, NEVER touches segments_.  Returns the number of
    /// segments built.  A failed build posts a FAILURE record (review M1) and
    /// returns -errno.
    int provisionReady(std::size_t depth, bool preallocate = true);
    /// Provisioning thread: tell the owner no segment will come for its pending
    /// demand (e.g. the node-wide reservation cap): the owner rolls inline.
    void reportProvisionFailure(int err) noexcept;
    /// Provisioning thread: ready segments queued and not yet taken by the
    /// owner (approximate while the owner runs; an estimate for depth / cap
    /// accounting only).  FAILURE records still queued are not counted (D19
    /// N6b: a queued failure must not stand in for a ready segment).
    [[nodiscard]] std::size_t readyQueuedApprox() const noexcept {
        const std::size_t all = readyRecordsQueued_();
        const std::size_t fails = queuedFailures_();
        return all > fails ? all - fails : 0;
    }
    /// Legacy single-slot form (the cleaner-driven provisioning): provisionReady(1).
    int preprovisionPending() { return provisionReady(1); }
    /// Owner: a ready segment is available (pulls the next record — a ready
    /// segment or a failure — into the owner-side front slot).
    [[nodiscard]] bool readyAvailable() noexcept { pullReady_(); return readyFront_ != nullptr; }
    /// Owner (tests): ready segments pooled (front slot + queued).
    [[nodiscard]] std::size_t readyCountForTest() const noexcept {
        return (readyFront_ != nullptr ? 1 : 0) + readyRecordsQueued_();
    }
    /// Owner (tests / observability): provisioning failures seen and the rolls
    /// they forced inline.
    [[nodiscard]] uint64_t provisionFailuresForTest() const noexcept { return provisionFailures_; }
    /// Provisioning thread: built segments dropped for a full ready ring (D19 N6d).
    [[nodiscard]] uint64_t readyDropsForTest() const noexcept { return readyDrops_; }
    [[nodiscard]] uint64_t failureInlineRollsForTest() const noexcept { return failureInlineRolls_; }
    /// Test seam (re-review N1): called inside beginColdPath_ between its
    /// rename-ring read and its done-ring drain — a test plays the provisioner
    /// finishing a rename exactly there.  Owner thread; null in production.
    void setColdPathInterleaveForTest(void (*fn)(void*), void* ctx) noexcept { coldSeamFn_ = fn; coldSeamCtx_ = ctx; }
    /// Provisioning thread (tests): make the next `n` builds fail with `err`.
    void setProvisionFaultForTest(int err, int n = 1) noexcept { provisionFaultErr_ = err; provisionFaultN_ = n; }

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

    /// TEST-ONLY: direct access to a segment by its base offset (e.g. to assert
    /// on LogSegment::hasReadMapForTest() / readMapFailures() for a specific
    /// sealed vs. active segment after recovery).  Returns nullptr if no segment
    /// is registered at that exact base offset.
    [[nodiscard]] LogSegment* segmentForTest(int64_t base) const noexcept {
        auto it = segments_.find(base);
        return it == segments_.end() ? nullptr : it->second.get();
    }

private:
    /// F3 fix-round-2, item B.3: segments_ must NEVER be empty once the Log is
    /// constructed — but relying on that invariant with a bare
    /// `*segments_.rbegin()->second` is silent UB the moment something
    /// upstream breaks it (e.g. a future reconstruction bug).  Hard-asserts
    /// (release-mode check + abort, not just a debug assert() that compiles
    /// out) instead of dereferencing an invalid iterator.  Defined in Log.cpp
    /// (needs spdlog; private + no external callers, so out-of-line costs
    /// nothing and keeps spdlog out of this header).
    LogSegment& activeSegment();
    void maybeRoll();
public:
    /// D28 test seam: the active segment's offset index.
    [[nodiscard]] const OffsetIndex* activeIndexForTest() { return activeSegment().indexForTest(); }
private:

    /// Rebuild and atomically publish a new sealed-segment snapshot.
    /// MUST be called on the single-writer append thread whenever segments_ changes:
    ///   - after maybeRoll() (when a segment seals)
    ///   - after drainCleanerSwaps() / reloadSegment() (after a segment swap)
    ///   - after drainRetentionDeletes() / deleteOldSegments() (segments erased)
    ///   - at the END of truncateTo() / reloadSegments() / recover()
    /// Only sealed segments (all except the last/active) are included; the active
    /// segment's size is mutable so it is excluded.
    void publishSealedSnapshot();
    /// reloadSegment() without the snapshot publish (drainCleanerSwaps batches
    /// several reloads under ONE publish).
    void reloadSegmentNoPublish_(int64_t baseOffset);
    /// Re-derive activeSegmentMaxTs_ from the active segment's on-disk batches
    /// (cold: recover / reloadSegments).
    void rescanActiveSegmentMaxTs_();

    std::string dir_; int64_t rollBytes_; int64_t logEndOffset_ = 0;
    uint64_t readViewCalls_ = 0;   // F18 B-2 (single-owner thread)
    uint64_t truncations_   = 0;   // contentGeneration() (owner thread)
    bool     recovered_     = false;   // see isRecovered()
    int32_t  leaderEpochStamp_ = -1;   // see setLeaderEpochStamp()
    LogStorageState storage_{};     ///< fail-stop state + inline-sync accounting (shared with segments)
    bool deferSealSync_ = false;
    static constexpr std::size_t kMaxPendingSeals = 16;
    std::array<LogSegment*, kMaxPendingSeals> pendingSeals_{};
    std::size_t pendingSealCount_ = 0;
    uint64_t droppedPendingSeals_ = 0;
    int64_t  truncationLowWater_ = kNoTruncation;   ///< see takeTruncationLowWater()
    void noteTruncatedTo_(int64_t leo) noexcept {
        if (leo < truncationLowWater_) truncationLowWater_ = leo;
    }
    /// M1: segments removed from segments_ while an async flush was in flight on
    /// their fd.  Destroyed (fd closed) by unpinSegmentFlush.  Rare, cold.
    std::vector<std::unique_ptr<LogSegment>> retiredSegments_;
    bool reloadAllocFaultForTest_ = false;   // F3 r7: test-only (cold path)
    uint32_t ownerTag_ = UINT32_MAX;
    using SegMap = LogSegmentMap;
    /// Wire `seg` into this log (storage state) and store it at `base`, retiring
    /// any pinned segment it replaces.
    void install_(int64_t base, std::unique_ptr<LogSegment> seg);
    /// Remove a segment, retiring it instead of closing its fd if pinned.
    SegMap::iterator erase_(SegMap::iterator it);
    void retire_(std::unique_ptr<LogSegment> seg);
    void throwIfFailed_() const;
    // ---- E1d recovery point state (owner thread) ----
    bool    crashedForTest_ = false;      ///< see crashForTest()
    bool    hadSegmentsAtOpen_ = false;   ///< segment files existed at construction (WARN gate)
    int64_t recoveryPoint_ = 0;
    int64_t persistedRp_   = kRecoveryPointUnknown;
    /// MAJOR-1 (fix round 2): the highest R carried by a checkpoint write that
    /// MAY have landed (submitted, outcome not yet observed — or observed by a
    /// previous owner and lost) and whose seq is above the last CONFIRMED
    /// durable write.  max(persistedRp_, this) bounds what restart recovery
    /// may read; a truncation below it is persisted inline first.  Reset only
    /// by a confirmed durable write with a higher seq.
    int64_t rpUnconfirmedMax_ = kRecoveryPointUnknown;
    bool    rpDue_         = false;   ///< R crossed a segment boundary since the last persist
    /// Hot-path B F7: a truncation to rpTruncCap_ waits for the on-disk R to be
    /// lowered to it (async, the owner's io backend).  INT64_MAX = none.
    int64_t rpTruncCap_    = INT64_MAX;
    int64_t rpEpochCap_    = INT64_MAX;   ///< review C1: see setEpochHistoryCap
    bool    epochGuard_    = false;       ///< review C1: see setEpochHistoryGuard
    int64_t startupRp_     = -1;          ///< see startupRecoveryPoint
    int64_t rpGuardTarget_ = -1;          ///< the recovered R held back by the guard
    bool    rpLowerDue_    = false;   ///< the on-disk R (upper bound) exceeds the truncation cap
    bool    rpKick_        = false;   ///< see takeCheckpointKick
    uint64_t rpLowerings_  = 0;
    uint64_t inPlaceTruncations_ = 0;
    // ---- review Md5: off-owner read-map growth (owner fields) -------------
    using MapRing = rune::consensus::ControlRing<256>;
    static constexpr std::int32_t kMapGrownMsg = 1, kMapFailedMsg = 2;
    MapRing  mapRing_;                    ///< provisioner → owner: grown mappings
    bool     asyncReadGrowth_ = false;
    bool     growOutstanding_ = false;
    int64_t  growBase_ = -1;
    uint64_t growGen_ = 0;
    uint32_t growWaits_ = 0;
    uint64_t readMapGrowths_ = 0, readMapGrowthFailures_ = 0, readMapsDiscarded_ = 0, ownerFallbackRemaps_ = 0;
    uint64_t readMapsRetiredAtSeal_ = 0;   ///< D30
    void retireSealedReadMap_(int64_t base, LogSegment& seg) noexcept;
    void disposeReadMap_(std::unique_ptr<MappedFile> m) noexcept;
    void noteSealedMapped_(int64_t base) noexcept;
    static constexpr std::size_t kMappedSealedCap = 8;   ///< D30 MAJOR-1: lazily mapped sealed segments kept
    std::array<int64_t, kMappedSealedCap> mappedSealed_{};
    std::size_t mappedSealedHead_ = 0, mappedSealedCount_ = 0;
    uint64_t sealedLazyRemaps_ = 0, sealedMapsEvicted_ = 0, readMapsDisposedInline_ = 0;   ///< D30 MEDIUM-3
    int64_t  growSeq_ = 0;   ///< re-review N2: the outstanding request's identity
    /// Owner: request a larger mapping for `seg` (keyed `base`) when it needs one.
    void maybeGrowReadMap_(LogSegment& seg, int64_t base, bool active) noexcept;
    /// Owner: install grown mappings handed back by the provisioner.
    void pollReadMaps_() noexcept;
    uint64_t cleanerFsyncs_ = 0, cleanerDirFsyncs_ = 0;   ///< cleaner thread (see cleanerFsyncsForTest)
    void clearTruncationCap_() noexcept { rpTruncCap_ = INT64_MAX; rpLowerDue_ = false; }
    uint64_t rpPersists_   = 0;
    uint64_t rpSeq_        = 0;       ///< highest slot seq ever written (or read)
    int      rpActiveSlot_ = -1;      ///< the slot holding persistedRp_ (-1 = none)
    int      rpFd_         = -1;      ///< the open, pre-allocated checkpoint file
    std::string rpPath_;              ///< precomputed (ctor) — no per-roll allocation
    std::string rpTmpPath_;
    struct RpCheckpoint {
        RpStage  stage     = RpStage::Idle;
        bool     abandoned = false;   ///< superseded: completion never moves persistedRp_
        int      slot      = -1;
        uint64_t seq       = 0;
        int64_t  value     = 0;
    } rp_;
    uint8_t rpImage_[kRpSlotBytes] = {};   ///< the Prepared slot image (stable until next prepare)
    // ---- E1d MEDIUM-2 directory durability (owner thread) ----
    int      dirFd_          = -1;      ///< the log directory, opened once at recovery
    uint64_t dirMutGen_      = 0;       ///< bumped by every directory mutation
    uint64_t dirSyncedGen_   = 0;       ///< highest generation a completed dir fsync covers
    bool     dirSyncInFlight_ = false;
    uint64_t dirSyncInFlightGen_ = 0;
    bool     rpDirBlocked_   = false;   ///< R wants to pass a not-yet-durable entry
    uint64_t dirSyncsDone_   = 0;
    /// Inline directory fsync (legacy / cold paths).  0 or the errno (fail-stop).
    int  syncDirectoryInline_() noexcept;
    /// Walk segments from R: advance over fully-durable segments, then into the
    /// first partially-durable one.  Sets rpDue_ on a boundary crossing.
    void advanceRecoveryPoint_() noexcept;
    void refreshRpDue_() noexcept;
    /// R crossed a boundary on an inline-sync path: persist inline unless the
    /// owner drives async checkpoints (deferSealSync_).
    void maybeCheckpointInline_() noexcept;
    /// Persist min(R, bound) inline when the on-disk value (or an in-progress
    /// checkpoint's) exceeds `bound` (F6: before a truncation deletes data).
    void persistAtMost_(int64_t bound) noexcept;
    /// Create the pre-allocated two-slot file holding `r` (tmp + fsync + rename
    /// + full dir fsync), open rpFd_.  Cold (first recovery).
    int  createRecoveryPointFile_(int64_t r) noexcept;
    /// recover(): the segment-chain validation from the recovery point onward.
    /// `deleteOnGap` false = a corrupt checkpoint (fail-stop): never delete.
    void validateFromRecoveryPoint_(int64_t rp, bool deleteOnGap);
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
    /// Base of the segment maybeRoll() just sealed (-1 = no seal in progress):
    /// identifies the newly-sealed entry explicitly (MEDIUM-2: a 0 timestamp is
    /// a real value, never a "not found" sentinel).
    int64_t newlySealedBase_ = -1;
    /// CP-5.1 (retention-after-restart fix): populated by recover() via a one-time
    /// per-segment batch scan.  Maps baseOffset → real largestTimestamp for every
    /// sealed segment found on disk at recovery time.  publishSealedSnapshot() uses
    /// this map as a fallback when a segment is not present in prevSnap AND
    /// newlySealedMaxTs_ == 0 (i.e. we are on the recover() publish path, not a
    /// hot-path maybeRoll() call).  The map is cleared after the first
    /// publishSealedSnapshot() call consumes it so it does not persist past recovery.
    std::map<int64_t, int64_t> recoveryMaxTimestamps_;
    SegMap segments_;
    /// hotpath-A: recycled map nodes — a roll's install_ reuses a node a
    /// retention drop / compaction / erase_ released, so a roll in steady
    /// state (with retention) allocates nothing.  Reserved once (ctor).
    static constexpr std::size_t kSpareSegNodes = 64;
    std::vector<SegMap::node_type> spareSegNodes_;
    /// Erase `it` (its LogSegment already moved out / released), keeping the
    /// node for reuse.  Returns the next iterator.
    SegMap::iterator eraseKeepNode_(SegMap::iterator it) noexcept;
    uint64_t segInstallGen_ = 0;   ///< F15 S4 N1: segment install generation

    // CP-6/race-fix: snapshot of sealed segment {baseOffset, sizeBytes}, written
    // exclusively on the append (owner) thread.  ownerSnap_ is the owner's view
    // (ownerSealedSnapshot()); the cleaner receives it through the D22 triple
    // buffer below (cleanerSnapshot()) — no lock (the former std::atomic_store /
    // atomic_load on a shared_ptr used libstdc++'s mutex pool on every roll).
    std::shared_ptr<const SealedSegmentSnapshot> ownerSnap_{std::make_shared<SealedSegmentSnapshot>()};
    // D22: owner → cleaner latest-value triple buffer.  slot snapBack_ is the
    // owner's, snapFront_ the cleaner's, the third is "middle" (index in
    // snapMiddle_, kSnapFresh = published since the cleaner last took it); a
    // slot changes hands only through the exchange.
    static constexpr uint8_t kSnapIndex = 3, kSnapFresh = 4;
    mutable std::shared_ptr<const SealedSegmentSnapshot> snapSlot_[3]{ownerSnap_, ownerSnap_, ownerSnap_};
    alignas(64) mutable std::atomic<uint8_t> snapMiddle_{1};
    uint8_t         snapBack_  = 0;   ///< owner thread
    mutable uint8_t snapFront_ = 2;   ///< cleaner thread
    // hotpath-A F4: the owner's side of the chunked snapshot (see
    // kSnapChunkEntries).  Live entries are the absolute indices
    // [snapBegin_, snapEnd_) of snapDir_; every published snapshot of any
    // directory sharing a chunk ends at or below snapEnd_, so slots >= snapEnd_
    // are written in place.  Owner thread only.
    std::shared_ptr<SealedSnapshotDir> snapDir_;
    std::size_t snapBegin_ = 0, snapEnd_ = 0, snapHeadersUsed_ = 0;
    uint64_t    snapRebuilds_ = 0;        ///< tail rebuilds (tests)
    uint64_t    snapRebuiltEntries_ = 0;  ///< entries written by tail rebuilds (tests)
    [[nodiscard]] const SegInfo& snapAt_(std::size_t a) const noexcept {
        return snapDir_->chunks[a / kSnapChunkEntries]->e[a % kSnapChunkEntries];
    }
    /// A fresh directory holding the live chunks of the current one, rebased
    /// so the first live chunk is chunk 0, with room for `extraChunks` more.
    void snapRebaseDir_(std::size_t keepChunks, std::size_t extraChunks);
    /// Write entry `e` at absolute slot snapEnd_ (unpublished) and advance.
    void snapPushBack_(const SegInfo& e);
    /// Publish [snapBegin_, snapEnd_) — fills the directory's next header and
    /// stores an aliasing shared_ptr (no allocation unless the pool is spent).
    void publishSnapshotView_();
    /// Roll: append the newly sealed segment's entry (O(1)).
    void snapshotAppendSealed_(SegMap::iterator sealed);
    /// Retention: the front `k` sealed segments were erased (O(1)).
    void snapshotDropFront_(std::size_t k);
    /// Rebuild the snapshot from the first entry whose segment may have changed
    /// — segments at or above `fromBase` (and the entry just below it: its end
    /// offset / timestamp floor may have moved).  Entries that compare equal
    /// are kept (shared); the rest of the tail is written fresh.
    void publishSealedSnapshotFrom_(int64_t fromBase);
    /// The cleaner's view of persistedRp_ (single writer: the owner).
    alignas(64) std::atomic<int64_t> cleanerPersistedRp_{kRecoveryPointUnknown};
    void setPersistedRp_(int64_t v) noexcept {
        persistedRp_ = v;
        cleanerPersistedRp_.store(v, std::memory_order_release);
    }

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
    static constexpr std::size_t kSwapRingCapacity = 256; // power of two (fix round 4: groups <= 64 members)
    static constexpr std::size_t kSwapRingMask     = kSwapRingCapacity - 1;
    /// A swap-ring slot.  kind Reload = legacy in-place reload of `base` (file
    /// already replaced by a synchronous / test caller).  kind Group = member of
    /// a deferred group; the FIRST member carries groupSize, seq, hasOutput.
    struct SegRef {
        int64_t  base = 0;
        uint64_t lineage = kAnyLineage;
        int64_t  nextOffset = 0;  ///< group head + hasOutput: the output's nextOffset
        uint32_t seq = 0;         ///< output name (unique among in-flight outputs)
        uint16_t groupSize = 0;   ///< 0 = legacy Reload entry; >0 on a group's first slot
        bool     hasOutput = false;
        LogSegment* prebuilt = nullptr;   ///< group head + hasOutput: the cleaner-opened output (fix round 4)
    };   // 40 bytes
    uint64_t cleanerGroupsRejected_ = 0;
    int64_t  cleanerRejectLowWater_ = INT64_MAX;   ///< see takeCleanerRejectLowWater()
    /// Validate + install one deferred group (slots [first, first+n)).  Owner thread.
    bool applyCleanedGroup_(std::size_t first, std::size_t n, int64_t maxBaseBound);
    /// Construction: delete uncommitted .cleaned outputs, complete committed .swap files.
    void recoverCleanerSwapFiles();
    /// The single SPSC producer path of the swap ring (n slots, one release-store).
    bool enqueueSwapSlots_(const SegRef* refs, std::size_t n) noexcept;
    /// Consumer body of drainCleanerSwaps over the published slots [head, tail).
    int drainSwapSlots_(std::size_t head, std::size_t tail, int64_t maxBaseBound);
    std::array<SegRef, kSwapRingCapacity> swapRing_{};
    alignas(64) std::atomic<std::size_t>   swapHead_{0}; // consumer index
    alignas(64) std::atomic<std::size_t>   swapTail_{0}; // producer index

    // CP-9: pending retention deletions — separate SPSC ring (to avoid mixing
    // compaction swaps with retention deletes; they have different semantics).
    // Producer = cleaner thread (enqueueDeletedSegment, after .log→.deleted rename).
    // Consumer = append/duty-cycle thread (drainRetentionDeletes → segments_.erase).
    static constexpr std::size_t kDeleteRingCapacity = 256;
    static constexpr std::size_t kDeleteRingMask     = kDeleteRingCapacity - 1;
    std::array<SegRef, kDeleteRingCapacity> deleteRing_{};
    uint64_t staleCleanerOps_ = 0;   ///< see staleCleanerOpsDropped() (owner thread)
    /// MAJOR-2: true when queued cleaner work for `base`/`lineage` may act — the
    /// segment exists, is NOT the active one, and (unless kAnyLineage) is the
    /// same incarnation the cleaner saw.  Owner thread.
    bool cleanerOpApplies_(SegMap::iterator it, uint64_t lineage) noexcept;
    alignas(64) std::atomic<std::size_t>    deleteHead_{0}; // consumer index
    alignas(64) std::atomic<std::size_t>    deleteTail_{0}; // producer index

    // ---- Fix round 4: owner → cleaner file-operation channel ----------------
    // The OWNER never touches the filesystem in a duty pass: it switches its
    // in-memory segments and posts what the files must become; the cleaner
    // performs the renames (+ directory fsync) and destroys the retired segment
    // objects (close / munmap — size-dependent costs) off the hot path.
    enum class MsgKind : uint8_t { SwapAccepted = 1, SwapRejected = 2, RetentionDrop = 3, Dispose = 4 };
    struct CleanerMsg {
        int64_t     base = 0;
        int64_t     end = 0;             ///< SwapAccepted: the group's exclusive end
        int64_t     logStartAfter = -1;  ///< RetentionDrop: the log start once applied
        LogSegment* obj = nullptr;       ///< object handed over (destroyed by the consumer)
        uint32_t    seq = 0;
        uint16_t    n = 0;               ///< SwapAccepted: number of Dispose slots that follow
        MsgKind     kind = MsgKind::Dispose;
        bool        hasOutput = false;
        uint8_t     reason = 0;          ///< SwapRejected: RejectReason (logged by the cleaner)
    };   // 40 bytes
    static constexpr std::size_t kMsgRingCapacity = 512;   // >= kSwapRingCapacity + 1
    static int64_t readLogStartCheckpoint(const std::string& dir);
    static constexpr std::size_t kMsgRingMask     = kMsgRingCapacity - 1;
    std::array<CleanerMsg, kMsgRingCapacity> msgRing_{};
    alignas(64) std::atomic<std::size_t> msgHead_{0};   // consumer index (cleaner; ~Log once it stopped)
    alignas(64) std::atomic<std::size_t> msgTail_{0};   // producer index (owner)
    /// Fix round 5 (no lock, no wait): the ONLY renames / unlinks of live
    /// segment names are the posted operations, performed by the cleaner.  An
    /// owner COLD path (truncate / reload / size retention) runs only when every
    /// posted operation has been performed (fileOpsQuiescent: head == tail,
    /// acquire) — the owner posts nothing while it waits (coldPathWanted_), so
    /// the cold path then owns every name.  Otherwise it is deferred to a later
    /// duty pass (PartitionReplica: FollowerSync TruncatePending).
    /// A deferred cold path pauses the NEXT swap drain and the NEXT retention
    /// drain (one each, then cleared): a waiter that retries every pass
    /// (FollowerSync::TruncatePending) re-arms it each pass; a one-shot caller
    /// never stalls posting for longer than one pass.
    bool pauseSwapPost_ = false;
    bool pauseDropPost_ = false;
    /// Persisted log start (log-start-offset-checkpoint).  Written by the cleaner
    /// while applying posted drops, or by the owner's quiescent cold path; the
    /// msg ring's release/acquire orders the two.
    int64_t persistedLogStart_ = 0;
    /// Cleaner → owner: errno of a failed log-start checkpoint write (0 = none).
    /// The owner fails the log (markStorageFailed) on its next drain.
    alignas(64) std::atomic<int> cleanerFailedErrno_{0};
    bool failNextCleanerApplyForTest_ = false;   ///< test seam (cleaner thread)
    uint64_t checkpointWriteFailures_ = 0;       ///< cleaner thread
    /// Owner: room for `n` messages?  (single producer; head acquire)
    bool msgRoom_(std::size_t n) const noexcept;
    /// Owner: publish `n` messages with ONE release-store (room checked first).
    void postMsgs_(const CleanerMsg* m, std::size_t n) noexcept;
    /// msgRing_ consumer (cleaner; or ~Log after the cleaner stopped): perform
    /// the posted operations in order.  `durable` = the renames happen (false
    /// only for a simulated crash: objects freed, disk untouched).  Stops — and
    /// retries next cycle — before a retention drop whose log-start checkpoint
    /// could not be written, or at an exception (head = the first unfinished
    /// message).  Returns whether any directory entry changed.
    bool applyCleanerMsgs_(int64_t nowMs, bool durable);
    /// Body of applyCleanerMsgs_: performs [i, tail), advancing i past each
    /// finished message (left at the first unfinished one on a stop / throw).
    bool applyCleanerMsgRange_(std::size_t& i, std::size_t tail, int64_t nowMs, bool durable);
    int rejectLogBudget_ = 64;   ///< cleaner thread: rejection log lines per Log (rate limit)
    /// Why the owner rejected a cleaner output (logged by the cleaner).
    enum class RejectReason : uint8_t { FrontGone = 1, NotContiguous, Active, Lineage, AboveBound,
                                        RenamePending, FlushInFlight, RecoveryPoint, BadOutput };
    /// Owner: may a cold path run now?  true = every posted operation is done
    /// (and coldPathWanted_ is cleared); false = deferred (posting paused).
    bool beginColdPath_(int64_t touchFrom) noexcept;   ///< touchFrom: lowest segment base touched
    /// Owner drains (swap / retention): false while a cold path waits for the
    /// cleaner to drain the msg ring (posting would starve it).  Also surfaces a
    /// cleaner checkpoint failure as a storage failure.
    bool ownerDrainAllowed_(bool& pause) noexcept;
    /// Cleaner (or owner cold path when quiescent): write log-start-offset-checkpoint (tmp + fsync + rename + dir fsync).
    bool writeLogStartCheckpoint_(int64_t v);
    static constexpr const char* kLogStartFile    = "log-start-offset-checkpoint";
    static constexpr const char* kLogStartTmpFile = "log-start-offset-checkpoint.tmp";

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
    /// <dir>/<kStagingPrefix><seq>.stlog / .stidx.  Reconciled at Log
    /// construction (recoverStagingFiles): an empty one is deleted; an adopted
    /// one is renamed to its base when that base fits the segment chain (D19
    /// N5a), else quarantined as .stlog.corrupt — kept for the operator and
    /// NEVER reclaimed automatically (N5b: both candidates' last offsets are
    /// logged for a duplicate base).
    static constexpr const char* kStagingPrefix = ".staging-";
    /// Monotonic counter so successive staging files get distinct names (avoids
    /// reusing a name whose fd/mmap the append thread may still hold post-adopt).
    /// Bg-thread-owned (only preprovisionPending writes it).
    std::size_t stagingSeq_ = 0;

    /// hotpath-A F2: the ready pool (see kMaxProvisionDepth).  Records:
    /// kReadyMsg {a = LogSegment* (owning)} and kFailedMsg {a = errno}.
    using ReadyRing = rune::consensus::ControlRing<512>;
    /// Records (ready or failure) queued on the ready ring (any thread, approximate).
    [[nodiscard]] std::size_t readyRecordsQueued_() const noexcept {
        const std::int64_t b = readyRing_.pendingBytes();
        return b <= 0 ? 0 : static_cast<std::size_t>(b) / ReadyRing::kRecordBytes;
    }
    /// Provisioning thread (D19 N6b): end positions of the last failure
    /// records offered (0 = none); one still queued has end > consumerPosition.
    static constexpr std::size_t kFailMarks = 32;   // >= the ring's record capacity
    static_assert(kFailMarks * ReadyRing::kRecordBytes >= 512, "a mark per ready-ring record");
    std::array<std::int64_t, kFailMarks> failMarkEnd_{};
    std::size_t failMarkNext_ = 0;
    uint64_t    readyDrops_ = 0;   ///< provisioning thread (D19 N6d)
    [[nodiscard]] std::size_t queuedFailures_() const noexcept {
        const std::int64_t consumed = readyRing_.consumerPosition();
        std::size_t n = 0;
        for (const std::int64_t e : failMarkEnd_) n += e > consumed ? 1 : 0;
        return n;
    }
    static constexpr std::int32_t kReadyMsg = 1, kFailedMsg = 2;
    ReadyRing   readyRing_;
    LogSegment* readyFront_ = nullptr;     ///< owner: next ready segment (taken off the ring)
    int         failFront_  = 0;           ///< owner: a pulled failure not yet acted on (errno)
    bool          provisioned_ = false;   ///< see setProvisioning (owner config)
    ProvisionChannel* provisionChannel_ = nullptr;
    int64_t       provisionThresholdBytes_ = 0;   ///< owner: Threshold demand point
    bool          thresholdPosted_ = false;       ///< owner: Threshold demand queued for this segment
    void (*coldSeamFn_)(void*) = nullptr;         ///< see setColdPathInterleaveForTest
    void* coldSeamCtx_ = nullptr;
    bool          adoptedPostPending_ = false;    ///< owner: an Adopted demand still to queue
    uint64_t      provisionFailures_ = 0;         ///< owner: failure records pulled
    uint64_t      failureInlineRolls_ = 0;        ///< owner: rolls made inline because of one
    int           provisionFaultErr_ = 0, provisionFaultN_ = 0;   ///< provisioner (tests)
    /// Owner: pull the next ready record into the front slot (no-op when one is there).
    void pullReady_() noexcept;
    /// Owner: post one demand (false = channel full, re-post later).
    bool postDemand_(ProvisionDemand kind) noexcept {
        return provisionChannel_->post(ProvisionChannel::kDemandMsgType,
                                       static_cast<int64_t>(reinterpret_cast<std::uintptr_t>(this)),
                                       static_cast<int64_t>(kind));
    }
    /// Owner: queue the demands that are due (threshold crossed / adoption).
    void postProvisionDemands_(int64_t activeBytes) noexcept;
    /// Owner: a blocked check (no ready segment): post the Hold demand at the
    /// first check of an episode and every kHoldRepostChecks checks after.
    void noteHoldCheck_() noexcept {
        if ((holdChecks_++ % kHoldRepostChecks) == 0 || holdPostPending_) {
            holdPostPending_ = provisionChannel_ != nullptr && !postDemand_(ProvisionDemand::Hold);
        }
    }
    uint32_t holdChecks_ = 0;        ///< owner: blocked checks in the current hold episode
    bool     holdPostPending_ = false;
    uint64_t      rollDeferrals_ = 0;     ///< see rollDeferralsForTest (owner)
    uint64_t      ownerPreallocs_ = 0;    ///< see ownerPreallocsForTest (owner)
    uint64_t      forcedRolls_    = 0;    ///< see forcedRollsForTest (owner)
    /// Overshoot cap of a deferred roll: rollBytes + clamp(rollBytes, 1 MiB,
    /// 256 MiB) — far below the 2 GiB segment position limit.
    [[nodiscard]] int64_t maxDeferredRollBytes_() const noexcept {
        // Review L1: never past the 32-bit segment position limit (minus room
        // for one maximal batch).
        const int64_t cap = rollBytes_ + std::clamp<int64_t>(rollBytes_, int64_t{1} << 20, int64_t{256} << 20);
        return std::min<int64_t>(cap, kMaxRollBytes);
    }

    /// Owner: room for one more rename IN FLIGHT — queued requests plus
    /// completions not yet drained (D19 N6c: reqTail - doneHead, both
    /// owner-written; every consumed request publishes exactly one completion,
    /// so finalizeStagingRenames always finds room on the done ring and never
    /// strands a request no one re-drives).
    [[nodiscard]] bool renameRoom_() const noexcept {
        return renameReqTail_.load(std::memory_order_relaxed) - renameDoneHead_.load(std::memory_order_relaxed) <
               kRenameRingCapacity;
    }
    /// Owner: take the oldest ready segment (nullptr = none).
    LogSegment* takeReady_() noexcept;
    /// rollDeferred() minus the size check.
    [[nodiscard]] bool rollBlockedNow_() noexcept {
        if (deferSealSync_ && pendingSealCount_ >= kMaxPendingSeals) return true;   // F3
        if (!provisioned_) return false;
        // F2: no ready segment — or no room to queue its staging -> base rename
        // (an adoption must never drop its rename: the segment would keep its
        // staging name / pathPending for good and retention would stall).
        // A pulled FAILURE (review M1) unblocks: the roll goes inline.
        postProvisionDemands_(rollBytes_);   // a demand lost to a full ring is re-posted here
        if (!readyAvailable()) {
            if (failFront_ != 0) return false;
            noteHoldCheck_();
            return true;
        }
        return !renameRoom_();
    }

    /// Adopt the oldest ready segment as the active segment at `base` (in-memory;
    /// enqueues the deferred rename, posts the provisioning demand), or — legacy
    /// (non-provisioned) logs only — synchronously create one if none is ready.
    /// Owner thread only.
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

static_assert(noexcept(std::declval<Log&>().readView(int64_t{0}, int32_t{0})),
              "Log::readView must be noexcept (F3)");

} // namespace rune::storage
