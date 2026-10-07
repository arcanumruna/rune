#pragma once
#include <rune/storage/LogReadView.h>
#include <rune/storage/OffsetIndex.h>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>
#include <memory>
#include <unistd.h>
#include <utility>
namespace rune::storage {

/// Per-Log storage health + inline-fsync accounting, owned by the Log and shared
/// (by pointer) with each of its segments.  Single owner thread (the Log's writer).
///   syncCalls      — every inline LogSegment::sync (test visibility: a shard loop
///                    must make none).
///   failedErrno    — fsyncgate fail-stop: the FIRST fsync/msync error of any kind
///                    on this log (inline or async).  Sticky — never clears; a later
///                    "successful" fsync cannot vouch for pages the kernel dropped.
///   faultForTest   — test-only: while non-zero every inline sync fails with it.
///   writeFaultForTest — test-only: while non-zero every data write (pwrite /
///                    pwritev) fails with it (E1: dead disk without any fsync).
struct LogStorageState {
    uint64_t syncCalls    = 0;
    uint64_t roleChangeSyncs = 0;   ///< recover()'s index msync + dir fsync (truncate / startup)
    int      failedErrno  = 0;
    int      faultForTest = 0;
    int      writeFaultForTest = 0;
    /// D20 review M1 test seam: when set, the durable append's pwrite retry loop
    /// (LogSegment::writeAll_) calls this instead of ::pwrite, so a test can force
    /// a short write / EINTR / hard errno.  Null in production (one predictable
    /// branch on the already syscall-bound durable path; not the pwrite hot path).
    ssize_t (*pwriteForTest)(int fd, const void* buf, std::size_t n, off_t off) = nullptr;
};

/// A single on-disk log segment: a ".log" data file paired with a ".index" mmap.
///
/// Durability policy:
///   append() calls sync() after every write (durable-by-default).
///   Every .log write goes straight to the page cache through the segment's one
///   POSIX fd (D20: no fstream / FILE*).  sync() fsyncs that
///   ".log" fd so bytes reach storage, then msyncs the ".index" mmap so
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
class LogSegment;
/// The owner's segment map (Log::SegMap).  Declared here so a provisioned
/// segment can carry the map node it will be installed into (D19 L2).
using LogSegmentMap = std::map<int64_t, std::unique_ptr<LogSegment>>;

class LogSegment {
public:
    /// @param indexMaxBytes  initial size of the mmap-backed .index.  Historically
    ///   a fixed 8 MiB (64x over-provisioned for a 64 MiB segment at 4 KiB
    ///   interval).  Now RIGHT-SIZED by Log (Log::indexMaxBytesFor) so the roll's
    ///   ftruncate + mmap + msync touch a few KiB, not 8 MiB.  OffsetIndex grows
    ///   the mapping on demand if a segment ever exceeds the estimate, so any
    ///   value is safe.  Defaulted to 8 MiB for callers that don't specify it.
    /// @param prefaultIndex  touch every index page at construction so the cold
    ///   minor-page-faults are paid HERE.  Set true by the background segment
    ///   pre-provisioner so the append/roll hot path never faults index pages.
    /// @param eagerReadMap  create the read-only .log mapping WITH the segment
    ///   (off the hot path) instead of lazily on first readView().  F3: only the
    ///   segment that IS or WILL BE active needs this — Log passes true when
    ///   creating a brand-new active segment or recovering the last (active)
    ///   segment.  Sealed segments recovered from disk default to false: N
    ///   retained sealed segments no longer cost N read mmaps + N fds up front.
    LogSegment(std::string dir, int64_t baseOffset, int32_t indexIntervalBytes = 4096,
               int32_t indexMaxBytes = 8 * 1024 * 1024, bool prefaultIndex = false,
               bool eagerReadMap = false, std::size_t readMapBytes = 0);
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

    /// Zero-copy restamp append (produce hot path).  Writes [data, data+len) to
    /// the page cache with the ASSIGNED base offset patched into bytes [0..7],
    /// WITHOUT any mutable copy of the batch: pwritev() scatter-gathers the
    /// 8-byte LE64 base offset followed by the read-only fragment tail
    /// [data+8, data+len).  The record-batch CRC covers [kAttributesOffset=21 ..
    /// end], so rewriting the base-offset field [0..7] is CRC-safe (identical to
    /// Kafka's leader restamp).  `data` is never mutated (may point directly at a
    /// read-only Aeron fragment / ring slot).  Same page-cache durability
    /// contract as appendNoSync().  Returns nextOffset (= lastOffset + 1).
    /// `leaderEpoch` >= 0 additionally patches the batch's partitionLeaderEpoch
    /// field [12..15] (CRC-excluded, Kafka's leader stamp) the same zero-copy way;
    /// -1 writes the producer's bytes there unchanged.
    int64_t appendNoSyncRestamp(const uint8_t* data, std::size_t len,
                                int64_t baseOffset, int64_t lastOffset,
                                int32_t leaderEpoch = -1);

    /// fsync ".log" + msync ".index" (D20: no libc buffer to flush).
    /// Called automatically by append() (durable-by-default).
    /// External callers (e.g. group-commit) may call this explicitly after
    /// batching multiple appendNoSync() calls.
    /// Throws std::system_error on ANY failure (fsync, index msync)
    /// and marks the owning Log permanently failed (LogStorageState::failedErrno).
    /// On an already-failed log it throws without touching the disk.
    void sync();

    /// E2 crash-simulation seam (DR tests): the .log byte length that the LAST
    /// successful inline sync() made durable in this incarnation (-1 = never
    /// synced since open).  A truncation below it lowers it.  Pure bookkeeping —
    /// one store on the (already syscall-bound) sync path; nothing reads it in
    /// production.  Async (io-backend) flushes are not tracked.
    [[nodiscard]] int64_t syncedBytesForTest() const noexcept { return syncedBytes_; }
    /// A segment object REBUILT over the same file (Log::truncateTo) keeps the
    /// synced extent of the prefix it took over.
    void inheritSyncedBytesForTest(int64_t b) noexcept { syncedBytes_ = b; }

    // ---- E1d durable recovery point ------------------------------------------
    /// One past the highest offset of this segment KNOWN durable on disk (every
    /// offset below it survives a power loss); == baseOffset() when nothing is
    /// known durable.  Raised by a successful inline sync(), by Log when an
    /// async (io backend) fdatasync of this segment's fd completes, and by Log
    /// recovery; lowered only by a truncation.  Owner thread; no syscall.
    [[nodiscard]] int64_t durableNext() const noexcept { return durableNext_; }
    /// Monotonic raise, clamped to the segment's next offset.
    void markDurableThrough(int64_t upTo) noexcept {
        if (upTo >= nextOffset_) {
            upTo = nextOffset_;
            syncedBytes_ = logSize_;   // E2 crash seam: the whole segment is on disk
        }
        if (upTo > durableNext_) durableNext_ = upTo;
    }
    /// Recovery / rebuild: set the durable extent exactly (clamped to
    /// [baseOffset, nextOffset]).
    void setDurableNext(int64_t v) noexcept {
        if (v > nextOffset_) v = nextOffset_;
        if (v < baseOffset_) v = baseOffset_;
        durableNext_ = v;
    }
    [[nodiscard]] bool fullyDurable() const noexcept { return durableNext_ >= nextOffset_; }
    /// Cold (recovery / truncation — accepted role-change sync work): fdatasync
    /// the .log fd so the CURRENT contents and size are durable, then mark the
    /// whole segment durable.  Counted in roleChangeSyncs (never syncCalls).
    /// An error is fail-stop (sticky failedErrno) and returns it; 0 on success.
    int syncForRecovery() noexcept;

    /// Wire the owning Log's storage state (set by Log when it installs the segment).
    void setStorageState(LogStorageState* s) noexcept { storage_ = s; }

    /// M1: async flushes in flight on this segment's fd.  The owning Log never
    /// closes (destroys) a segment while this is > 0 — it is retired and closed
    /// on the completion that unpins it.  Owner thread only.
    void     pinFlush() noexcept { ++flushPins_; }
    uint32_t unpinFlush() noexcept { return flushPins_ > 0 ? --flushPins_ : 0; }
    [[nodiscard]] uint32_t flushPins() const noexcept { return flushPins_; }

    /// Seal hook before the fd is handed to the io backend.  D20: writes are
    /// unbuffered (pwrite), so there is nothing to push — always 0.
    [[nodiscard]] int flushBufferedToPageCache() noexcept;

    std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes);

    /// Zero-copy read: returns a span into a read-only mmap of the .log covering
    /// whole record batches starting at the first batch whose lastOffset >=
    /// startOffset, up to `maxBytes` (always at least one batch — progress
    /// guarantee).  No heap allocation, no pread copy.  Returns an empty view if
    /// startOffset is at/after the segment's next offset.  See LogReadView for
    /// lifetime rules.  Non-const: may lazily create / grow-remap the read mmap.
    /// noexcept (F3): a failed lazy map (ENOMEM, missing file, max_map_count,
    /// ...) returns an empty LogReadView instead of throwing — callers already
    /// treat empty as "nothing now; retry next cycle".  See readMapFailures().
    LogReadView readView(int64_t startOffset, int32_t maxBytes) noexcept;
    /// F15 S4 fix round 1 (cold-cache build): the read-only mapping of the
    /// written bytes [0, sizeBytes()) WITHOUT touching any page (an mmap /
    /// remap syscall at most; no index lookup, no header read).  The caller
    /// gates every access on page residency.  Owner (append) thread; valid
    /// until the next append / roll / truncate on this segment.  noexcept like
    /// readView (F3): a failed lazy map returns an empty view.
    LogReadView mappedBytes() noexcept;

    /// `syncIndex` = false (hot-path B F7, a worker-driven log's truncation):
    /// the rebuilt index is renamed into place without msync / directory fsync
    /// (the owner gates R on an async directory fsync instead).
    int64_t recover(bool syncIndex = true);
    /// Hot-path B review M2: truncate THIS (active) segment in place at the
    /// batch holding `truncateOffset` (rounded down, as truncateTo): one
    /// ftruncate of the open fd, the index entries past the cut zeroed in its
    /// mapping, the cursors rewound.  No new file, no rename, no directory
    /// change (so no directory fsync), no file read (header walk over the read
    /// mapping).  `keptMaxTs` = the kept prefix's max timestamp (ms).
    /// 0 = done; > 0 = errno (ftruncate failed — nothing changed); -1 = not
    /// applicable (no read mapping): the caller rebuilds the segment.
    int truncateInPlace(int64_t truncateOffset, int64_t& keptMaxTs) noexcept;
    /// Fix round 3 (O(1) owner-thread install): adopt a SEALED segment whose
    /// .log and .index were built, CRC-verified and fsynced by the cleaner
    /// thread — no re-read, no CRC, no index rebuild, no fsync.  `nextOffset`
    /// comes from the cleaner; the size is one fstat.  The segment is fully
    /// durable (the cleaner fsynced it).  Returns 0 or the errno of the fstat.
    int openTrusted(int64_t nextOffset) noexcept;

    int64_t baseOffset() const noexcept { return baseOffset_; }
    int64_t sizeBytes() const noexcept { return logSize_; }
    int64_t nextOffset() const noexcept { return nextOffset_; }
    std::string logPath() const { return logPath_; }
    /// Return the POSIX fd of the open .log file (the same fd used by fdatasync
    /// in sync()).  Valid for the ACTIVE segment only; -1 if not yet opened.
    /// Used by the async flusher (IoBackend) to call fdatasync off the worker
    /// hot path without the worker calling sync() directly.
    int logFd() const noexcept { return fd_; }
    /// F15 S4 fix round 2 (N1): a Log-wide monotonic install generation (set by
    /// Log::install_) — the segment's identity, never reused (no pointer ABA).
    uint64_t installGen() const noexcept { return installGen_; }
    void setInstallGen(uint64_t g) noexcept { installGen_ = g; }
    /// Content lineage for the sealed-snapshot timestamp cache (Log::
    /// publishSealedSnapshot) and the cleaner's stale-work check: a cached
    /// largestTimestamp is reused, and a queued cleaner swap / retention delete
    /// is applied, only for the SAME lineage at the same base.  Every install
    /// (roll, truncate rebuild, re-create, compaction rewrite) gets a fresh
    /// lineage; only a full reloadSegments() of an unchanged file inherits it.
    /// A compaction rewrite is NOT "the same content": a coalesced merge holds
    /// the later members' (newer) records — its timestamp comes from tsFloor().
    uint64_t tsLineage() const noexcept { return tsLineage_; }
    void setTsLineage(uint64_t l) noexcept { tsLineage_ = l; }
    /// Lower bound for this segment's published largestTimestamp: the max of
    /// what the segment(s) it replaced / absorbed published (a compaction
    /// rewrite inherits its predecessor's; a merge member dropped from the map
    /// folds its own into the segment that now holds its records).
    int64_t tsFloor() const noexcept { return tsFloor_; }
    void raiseTsFloor(int64_t ts) noexcept { if (ts > tsFloor_) tsFloor_ = ts; }
    /// The largestTimestamp the last sealed-snapshot publish gave this segment.
    int64_t publishedTs() const noexcept { return publishedTs_; }
    void setPublishedTs(int64_t ts) noexcept { publishedTs_ = ts; }
    /// E1d MEDIUM-2: the owning Log's directory-mutation generation at which
    /// this segment's directory entry was last created / renamed (0 = present
    /// at open: made durable by recovery's directory fsync).  The entry is
    /// durable once a directory fsync covering this generation completed.
    uint64_t dirEntryGen() const noexcept { return dirEntryGen_; }
    void setDirEntryGen(uint64_t g) noexcept { dirEntryGen_ = g; }

    /// #5 non-blocking append: preallocate `bytes` of disk blocks for the .log
    /// WITHOUT changing the file size, so subsequent O_APPEND pwrite()s never stall
    /// on block allocation (the append-p99 tail).  Linux: fallocate(FALLOC_FL_KEEP_SIZE);
    /// macOS: fcntl(F_PREALLOCATE).  Best-effort + idempotent (no-op on failure or
    /// unsupported fs).  Call on the ACTIVE (appended) segment only.
    void preallocate(int64_t bytes) noexcept;

    // -----------------------------------------------------------------------
    // Background segment pre-provisioning (append-p99 fix).
    //
    // The expensive part of a segment roll is CREATING the next segment: 2-3
    // open()s + ftruncate + mmap (+cold page-faults) of the .index.  On APFS a
    // single open() is ~35µs p50 / 62µs p99 with multi-ms tail spikes, so doing
    // this on the append/roll hot path is what produces the append-p99 tail.
    //
    // Fix: a BACKGROUND thread pre-builds a staging segment (files opened,
    // index ftruncated + mmap'd + prefaulted) UNDER A TEMPORARY NAME ahead of
    // time.  At roll the append thread ADOPTS it: a single rename() of the
    // staging .log → <base>.log (rename doesn't disturb the already-open fd or
    // the index mmap) plus an O(1) in-memory rebase.  No open()/ftruncate/mmap
    // on the hot path.  The seal fsync of the OUTGOING segment stays fully
    // synchronous (durability unchanged) — only the NEXT (empty) segment's
    // creation moves off the hot path.
    //
    // The staging .index keeps its temporary name while the segment is active
    // (the in-memory mmap works regardless of the on-disk filename, and
    // recover() ALWAYS rebuilds .index fresh from .log so the on-disk index
    // name is irrelevant to correctness).  Orphaned staging files left by a
    // crash are swept at Log construction (see Log::kStagingPrefix handling).
    // -----------------------------------------------------------------------

    /// Build a staging segment on a BACKGROUND thread.  Creates
    /// <dir>/<stagingName>.log and <dir>/<stagingName>.index, opens the fds and
    /// mmaps + prefaults the (right-sized) index.  baseOffset is provisional (0);
    /// call adoptAsActive(base) on the append thread to rebase + rename at roll.
    /// Fix round 4: open a finished, cleaner-built SEALED segment from its
    /// temporary files (cleaner thread) — fds + index mmap + entry count + size,
    /// adopted TRUSTED (fully durable: the cleaner fsynced it).  logPath() is
    /// the CANONICAL <base>.log the cleaner renames the files to after the owner
    /// accepts; until then everything goes through the open fd / mmap (a rename
    /// never affects an open inode).  Throws on any open / map failure.
    static std::unique_ptr<LogSegment> openPrebuilt(const std::string& dir, int64_t base,
                                                    const std::string& logFile,
                                                    const std::string& indexFile,
                                                    int64_t nextOffset, int32_t indexIntervalBytes,
                                                    int32_t indexMaxBytes);
    static std::unique_ptr<LogSegment> prepareStaging(
        const std::string& stagingLogPath, const std::string& stagingIndexPath,
        int64_t stagingSeq, int32_t indexIntervalBytes, int32_t indexMaxBytes,
        std::size_t readMapBytes = 0);
    /// Hot-path B F5 + review Md5: the read mapping a segment that grows to
    /// `rollBytes` ends with — page-rounded rollBytes + one maximal batch (never
    /// a power of two).  Segments start with kReadMapMinBytes (an idle
    /// partition costs 1 MiB of address space) and a provisioned log grows the
    /// mapping OFF the owner (SegmentProvisionerAgent; Log::readView never
    /// remaps on the owner — a munmap + mmap of tens of MiB measured 100-260 us
    /// on the worker).
    [[nodiscard]] static std::size_t readMapBytesFor(int64_t rollBytes) noexcept;
    static constexpr std::size_t kReadMapSlackBytes = 1u << 20;
    /// Bytes currently mapped for readView.
    [[nodiscard]] std::size_t readMapBytes() const noexcept { return readMapSize_; }
    /// false: readView serves only the mapped prefix and never (re)maps on the
    /// caller's thread once a mapping exists (the owner of a provisioned log).
    void setOwnerRemap(bool on) noexcept { ownerRemap_ = on; }
    /// Install a larger mapping built off-thread; returns the previous one (to
    /// be destroyed off-thread).  Owner thread.
    std::unique_ptr<MappedFile> swapReadMap(std::unique_ptr<MappedFile> m) noexcept;

    /// The staging sequence number this segment was pre-provisioned under (so the
    /// append thread can tell the background thread which staging files to rename
    /// after adoption).  Only meaningful for a pre-provisioned segment.
    int64_t stagingSeq() const noexcept { return stagingSeq_; }

    /// D19 L2: an empty map node built with the segment off the owner (the
    /// provisioner); the owner installs the adopted segment into it instead of
    /// allocating one (logs without retention never release a node to reuse).
    void setSpareMapNode(LogSegmentMap::node_type nh) noexcept { spareMapNode_ = std::move(nh); }
    [[nodiscard]] LogSegmentMap::node_type takeSpareMapNode() noexcept { return std::move(spareMapNode_); }

    /// Adopt a staging segment as the live segment at `base` (append thread only).
    /// PURE IN-MEMORY rebase — ZERO syscalls (no rename/open/mmap).  The staging
    /// files keep their temp names on disk; writes go through the already-open fd
    /// + index mmap (both name-agnostic).  The disk rename to <base>.{log,index}
    /// is deferred to the background thread.  MUST be called exactly once, before
    /// any append to this segment.
    void adoptAsActive(int64_t base);

    /// True if this segment is still in the staging (un-adopted) state.
    bool isStaging() const noexcept { return staging_; }
    /// Adopted from staging and the staging→<base> rename not yet confirmed
    /// (Log::drainStagingRenamesDone → setLogPath).  In-memory, no syscall.
    bool pathPending() const noexcept { return pathPending_; }
    /// hotpath-A: setLogPath(<dir>/<20-digit base>.log) composed IN PLACE —
    /// no allocation when the capacity was reserved off-thread (prepareStaging
    /// reserves it), so the owner's rename-done drain allocates nothing.
    void setCanonicalLogPath(const std::string& dir, int64_t base) noexcept;

    /// TEST-ONLY: whether the read-only .log mapping currently exists.  A
    /// sealed segment recovered from disk starts false (F3: lazy mapping) and
    /// flips true after its first successful readView().
    [[nodiscard]] bool hasReadMapForTest() const noexcept { return static_cast<bool>(readMap_); }
    /// TEST SEAM (D3b r2): make every read-map creation fail.  The read map is now
    /// made through the segment's own fd (never by path), so a test can no longer
    /// break it by swapping the .log path out from under the segment.
    void setReadMapFaultForTest(bool on) noexcept { readMapFaultForTest_ = on; }
    /// Count of readView() calls that failed to (re)establish the read mapping
    /// (ensureReadMap() returned false), e.g. because the backing file is gone
    /// or the map/open syscall failed.  Monotonically increasing; never reset.
    [[nodiscard]] uint64_t readMapFailures() const noexcept { return readMapFailures_; }
    /// Hot-path B F5: read mappings created / grown on demand by readView (the
    /// caller's thread — the owner's, for a push / fetch).  0 for a segment
    /// created whole-segment-mapped and never outgrowing it.
    [[nodiscard]] uint64_t readMapRemaps() const noexcept { return readMapRemaps_; }

    /// Update the in-memory .log path after the background thread renamed the
    /// staging file to its final <base>.log on disk (append thread only).  The
    /// open fd_ / mmap are unaffected by the rename; this only keeps logPath_
    /// (used by recover()/reload) in sync with the on-disk name.
    void setLogPath(const std::string& p) noexcept;

private:
    /// Empty ctor used only by prepareStaging() (fields set by the factory).
    LogSegment() : baseOffset_(0), indexIntervalBytes_(4096),
                   indexMaxBytes_(8 * 1024 * 1024), nextOffset_(0) {}
    static std::string fname(std::string dir, int64_t base, const char* ext);
    /// Shared write core: update index, write bytes, advance nextOffset_.
    /// Does NOT call sync(). Called by append() and appendNoSync().
    int64_t writeAndUpdateState(const uint8_t* data, std::size_t len, int64_t lastOffset);
    /// D20: pwrite all of [data, data+len) at logSize_ through fd_ (EINTR /
    /// short-write retry).  0 or errno.
    int writeAll_(const uint8_t* data, std::size_t len) noexcept;
    [[noreturn]] void failWrite_(int err, const char* what);   ///< E1: sticky fail-stop + throw

    /// (Re)map the .log read-only to cover the currently-written logSize_ bytes so
    /// readView() can return a span into the OS page cache.  Maps with geometric
    /// headroom past EOF, so the active segment only remaps when it outgrows it.  No-op for logSize_==0
    /// (mmap of length 0 is EINVAL).  The read mmap shares the page cache with the
    /// writer's fd, so appendNoSync()/flushed append() bytes are visible at once.
    /// noexcept (F3): catches everything internally.  Returns true if the map is
    /// ready (including the logSize_==0 / already-sufficient no-op cases), false
    /// on failure — readMap_/readMapSize_ are left null/0 (never a dangling or
    /// stale mapping) and readMapFailures_ is bumped, with a rate-limited spdlog
    /// warn (first failure, then every 1024th).
    std::unique_ptr<MappedFile> makeReadMap_(std::size_t bytes);   // D3b r2: through fd_, never by path
    bool ensureReadMap() noexcept;
    /// Eagerly create the read mapping at segment construction (off the hot path).
    void openReadMap(std::size_t bytes = 0) noexcept;
    int64_t baseOffset_; int32_t indexIntervalBytes_;
    int32_t indexMaxBytes_;   // right-sized initial .index mmap size (grows on demand)
    std::string logPath_;   // D20: no std::fstream — fd_ is the only .log handle
    LogStorageState* storage_ = nullptr;  // owning Log's health/accounting (may be null)
    uint32_t flushPins_ = 0;              // M1: async flushes in flight on fd_
    int fd_ = -1;   // the .log's only handle: pwrite/pwritev/pread/ftruncate/fsync/read-mmap
    std::unique_ptr<OffsetIndex> index_;
    uint64_t installGen_ = 0;
    uint64_t tsLineage_ = 0;     ///< see tsLineage()
    int64_t  tsFloor_ = INT64_MIN;     ///< see tsFloor()
    int64_t  publishedTs_ = INT64_MIN; ///< see publishedTs()
    uint64_t dirEntryGen_ = 0;   ///< E1d MEDIUM-2: see dirEntryGen()
    int64_t logSize_ = 0; int64_t nextOffset_; int64_t bytesSinceIndex_ = 0;
    int64_t syncedBytes_ = -1;   ///< see syncedBytesForTest()
    int64_t durableNext_ = 0;    ///< E1d: see durableNext() (== baseOffset_ when none)
    bool staging_ = false;   // true until adoptAsActive() rebases a pre-provisioned segment
    bool pathPending_ = false;   // see pathPending()
    int64_t stagingSeq_ = -1; // pre-provision staging sequence (see stagingSeq())
    LogSegmentMap::node_type spareMapNode_;   // see setSpareMapNode (empty when none)

    // Read-only mmap of the .log for zero-copy readView().  Created WITH the
    // segment (openReadMap; lazily only if that failed) with headroom past EOF,
    // grow-remapped (geometrically) only when the active segment outgrows it.  Separate O_RDONLY fd from the writer's fd_, but
    // the same inode → shared page cache, so it observes committed appends.
    std::unique_ptr<MappedFile> readMap_;
    std::size_t readMapSize_ = 0;   // bytes currently mapped by readMap_ (>= logSize_ headroom)
    static constexpr std::size_t kReadMapMinBytes = 1u << 20;   // first read mapping: 1 MiB
    bool     readMapFaultForTest_ = false;   // D3b r2 test seam (setReadMapFaultForTest)
    uint64_t readMapRemaps_ = 0;
    bool     ownerRemap_ = true;
    uint64_t readMapFailures_ = 0;  // count of failed ensureReadMap() calls — see readMapFailures()
};

static_assert(noexcept(std::declval<LogSegment&>().readView(int64_t{0}, int32_t{0})),
              "LogSegment::readView must be noexcept (F3)");

} // namespace rune::storage
