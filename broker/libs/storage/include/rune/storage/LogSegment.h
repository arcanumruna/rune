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

/// Zero-copy read result: a span into a segment's read-only mmap covering one or
/// more WHOLE record batches (never a partial trailing batch).  No heap
/// allocation, no copy — the bytes are the OS page-cache pages shared with the
/// writer's fd.
///
/// Lifetime: valid until the next readView() on the SAME segment triggers a
/// remap-on-grow, or the segment is destroyed/reloaded.  A caller must consume
/// (encode/push) the span before issuing the next readView() on that segment.
struct LogReadView {
    const uint8_t* data       = nullptr; // pointer into the segment's read mmap
    std::size_t    len        = 0;       // bytes spanned (whole record batches)
    int64_t        baseOffset = 0;       // baseOffset of the FIRST batch in the view
    int64_t        nextOffset = 0;       // lastOffset of the LAST batch + 1 (cursor advance)
    bool empty() const noexcept { return data == nullptr || len == 0; }
};

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
    /// @param indexMaxBytes  initial size of the mmap-backed .index.  Historically
    ///   a fixed 8 MiB (64x over-provisioned for a 64 MiB segment at 4 KiB
    ///   interval).  Now RIGHT-SIZED by Log (Log::indexMaxBytesFor) so the roll's
    ///   ftruncate + mmap + msync touch a few KiB, not 8 MiB.  OffsetIndex grows
    ///   the mapping on demand if a segment ever exceeds the estimate, so any
    ///   value is safe.  Defaulted to 8 MiB for callers that don't specify it.
    /// @param prefaultIndex  touch every index page at construction so the cold
    ///   minor-page-faults are paid HERE.  Set true by the background segment
    ///   pre-provisioner so the append/roll hot path never faults index pages.
    LogSegment(std::string dir, int64_t baseOffset, int32_t indexIntervalBytes = 4096,
               int32_t indexMaxBytes = 8 * 1024 * 1024, bool prefaultIndex = false);
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

    /// Zero-copy read: returns a span into a read-only mmap of the .log covering
    /// whole record batches starting at the first batch whose lastOffset >=
    /// startOffset, up to `maxBytes` (always at least one batch — progress
    /// guarantee).  No heap allocation, no pread copy.  Returns an empty view if
    /// startOffset is at/after the segment's next offset.  See LogReadView for
    /// lifetime rules.  Non-const: may lazily create / grow-remap the read mmap.
    LogReadView readView(int64_t startOffset, int32_t maxBytes);

    int64_t recover();

    int64_t baseOffset() const noexcept { return baseOffset_; }
    int64_t sizeBytes() const noexcept { return logSize_; }
    int64_t nextOffset() const noexcept { return nextOffset_; }
    std::string logPath() const { return logPath_; }
    /// Return the POSIX fd of the open .log file (the same fd used by fdatasync
    /// in sync()).  Valid for the ACTIVE segment only; -1 if not yet opened.
    /// Used by the async flusher (IoBackend) to call fdatasync off the worker
    /// hot path without the worker calling sync() directly.
    int logFd() const noexcept { return fd_; }

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
    static std::unique_ptr<LogSegment> prepareStaging(
        const std::string& stagingLogPath, const std::string& stagingIndexPath,
        int64_t stagingSeq, int32_t indexIntervalBytes, int32_t indexMaxBytes);

    /// The staging sequence number this segment was pre-provisioned under (so the
    /// append thread can tell the background thread which staging files to rename
    /// after adoption).  Only meaningful for a pre-provisioned segment.
    int64_t stagingSeq() const noexcept { return stagingSeq_; }

    /// Adopt a staging segment as the live segment at `base` (append thread only).
    /// PURE IN-MEMORY rebase — ZERO syscalls (no rename/open/mmap).  The staging
    /// files keep their temp names on disk; writes go through the already-open fd
    /// + index mmap (both name-agnostic).  The disk rename to <base>.{log,index}
    /// is deferred to the background thread.  MUST be called exactly once, before
    /// any append to this segment.
    void adoptAsActive(int64_t base);

    /// True if this segment is still in the staging (un-adopted) state.
    bool isStaging() const noexcept { return staging_; }

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

    /// (Re)map the .log read-only to cover the currently-written logSize_ bytes so
    /// readView() can return a span into the OS page cache.  Grow-remap when the
    /// active segment has grown past the current mapping.  No-op for logSize_==0
    /// (mmap of length 0 is EINVAL).  The read mmap shares the page cache with the
    /// writer's fd, so appendNoSync()/flushed append() bytes are visible at once.
    void ensureReadMap();
    int64_t baseOffset_; int32_t indexIntervalBytes_;
    int32_t indexMaxBytes_;   // right-sized initial .index mmap size (grows on demand)
    std::string logPath_; std::fstream log_;
    int fd_ = -1;   // POSIX fd for fdatasync — mirrors log_ (write path uses fstream)
    std::unique_ptr<OffsetIndex> index_;
    int64_t logSize_ = 0; int64_t nextOffset_; int64_t bytesSinceIndex_ = 0;
    bool staging_ = false;   // true until adoptAsActive() rebases a pre-provisioned segment
    int64_t stagingSeq_ = -1; // pre-provision staging sequence (see stagingSeq())

    // Lazy read-only mmap of the .log for zero-copy readView().  Created on the
    // first readView() call (never for a write-only segment), grow-remapped when
    // the active segment grows.  Separate O_RDONLY fd from the writer's fd_, but
    // the same inode → shared page cache, so it observes committed appends.
    std::unique_ptr<MappedFile> readMap_;
    std::size_t readMapSize_ = 0;   // bytes currently mapped by readMap_
};
} // namespace rune::storage
