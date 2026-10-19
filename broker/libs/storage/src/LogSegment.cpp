#include <rune/storage/LogSegment.h>
#include <rune/storage/Timestamps.h>
#include <rune/protocol/RecordBatch.h>
#include <algorithm>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/uio.h>
#include <cstdint>
#include <stdexcept>
#include <iomanip>
#include <cstdio>
#include <sstream>
#include <filesystem>
#include <system_error>
#include <cerrno>
#include <spdlog/spdlog.h>
namespace rune::storage {

namespace {
// Unified sparse-index sampling decision.
// Returns true if a batch at the current bytesSinceIndex_ cursor should get an
// index entry.  Used identically by append(), recover(), and LogCleaner::buildIndex()
// so that post-recovery index entries are byte-for-byte reproducible (fixes C2).
//
// Rule: index the very first batch in the segment/session (bytesSince == 0),
// or when accumulated bytes since the last entry reach the interval.
inline bool shouldIndex(int64_t bytesSince, int32_t interval) noexcept {
    return bytesSince == 0 || bytesSince >= static_cast<int64_t>(interval);
}
} // anonymous namespace

std::string LogSegment::fname(std::string dir, int64_t base, const char* ext) {
    std::ostringstream os; os << dir << "/" << std::setw(20) << std::setfill('0') << base << ext;
    return os.str();
}

// ---------------------------------------------------------------------------
// Background pre-provisioning (append-p99 fix).
// ---------------------------------------------------------------------------
std::unique_ptr<LogSegment> LogSegment::openPrebuilt(
    const std::string& dir, int64_t base, const std::string& logFile, const std::string& indexFile,
    int64_t nextOffset, int32_t indexIntervalBytes, int32_t indexMaxBytes) {
    auto seg = std::unique_ptr<LogSegment>(new LogSegment());
    seg->baseOffset_         = base;
    seg->indexIntervalBytes_ = indexIntervalBytes;
    seg->indexMaxBytes_      = indexMaxBytes;
    seg->nextOffset_         = base;
    seg->durableNext_        = base;
    // D20: the raw fd is the segment's ONLY handle on its .log (no FILE*/fstream —
    // glibc's fclose walks the process-wide FILE list: O(open segments)).
    seg->fd_ = ::open(logFile.c_str(), O_RDWR | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
    if (seg->fd_ < 0)
        throw std::system_error(errno, std::system_category(), "LogSegment::openPrebuilt: open " + logFile);
    seg->index_ = std::make_unique<OffsetIndex>(indexFile, base, indexMaxBytes);
    if (const int e = seg->openTrusted(nextOffset); e != 0)
        throw std::system_error(e, std::system_category(), "LogSegment::openPrebuilt: fstat " + logFile);
    seg->logPath_ = fname(dir, base, ".log");   // the name it will have once installed
    return seg;
}

std::unique_ptr<LogSegment> LogSegment::prepareStaging(
    const std::string& stagingLogPath, const std::string& stagingIndexPath,
    int64_t stagingSeq, int32_t indexIntervalBytes, int32_t indexMaxBytes, std::size_t readMapBytes) {
    // Construct at provisional base 0.  All the expensive syscalls (1 open() for
    // the .log fd, 1 open() + ftruncate + mmap + prefault for the .index)
    // happen HERE, on the background thread.  The staging files use non-.log/.index
    // extensions (invisible to segment/retention scans); the adopt-time rename
    // gives them their final <base>.log/.index names.
    auto seg = std::unique_ptr<LogSegment>(new LogSegment());
    seg->baseOffset_        = 0;
    seg->indexIntervalBytes_= indexIntervalBytes;
    seg->indexMaxBytes_     = indexMaxBytes;
    seg->nextOffset_        = 0;
    seg->staging_           = true;
    seg->stagingSeq_        = stagingSeq;
    // hotpath-A: room for the canonical <dir>/<20-digit base>.log the owner
    // composes in place at the rename-done drain (setCanonicalLogPath).
    seg->logPath_.reserve(stagingLogPath.size() + 32);
    seg->logPath_           = stagingLogPath;
    const std::string& idxPath = stagingIndexPath;
    seg->fd_ = ::open(seg->logPath_.c_str(), O_RDWR | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
    // F3 fix-round-1, item 2: an unchecked open() failure used to leave fd_ at
    // -1 and silently continue — sync() would then silently skip the
    // fdatasync forever (fd_ < 0 is a no-op in sync()), a silent durability
    // loss.  Surface it exactly like the other construction failures below
    // (OffsetIndex, MappedFile) already do: throw.  The caller
    // (Log::preprovisionPending) already wraps this call in try/catch(...)
    // and retries next cycle — unchanged by this fix.
    if (seg->fd_ < 0) {
        throw std::system_error(errno, std::system_category(),
            "LogSegment::prepareStaging: open failed for " + seg->logPath_);
    }
    // Right-sized index, PREFAULTED so the append/roll hot path never faults it.
    seg->index_ = std::make_unique<OffsetIndex>(idxPath, /*base*/0, indexMaxBytes,
                                                /*prefault*/true);
    // D26 (fix65 review LOW-1): no read mapping by default.  Push / fetch
    // reads pread (#65 / D26); only the cold mapped readers (RC rebuild, PSM
    // rebuild, truncation prep, the rare too-large fallback) map — so a
    // staged segment's mmap (+ its munmap at seal) was dead mmap_lock work for
    // every push-only log.  For a log WITH a mapped reader the provisioner
    // maps the new active segment off the owner, requested at the first
    // append after the roll (lows A/B/C review M3); only a readView that beats
    // that answer maps on the owner (an mmap sized to cover the active
    // segment — counted in ownerFallbackRemaps).
    if (readMapBytes > 0) seg->openReadMap(readMapBytes);
    return seg;
}

void LogSegment::adoptAsActive(int64_t base) {
    // Append thread only.  PURE IN-MEMORY rebase — ZERO syscalls on the hot path.
    // The staging .log / .index files keep their temporary names on disk for now;
    // all writes go through the already-open fd_ (pwrite) and the index mmap, both
    // of which are name-agnostic.  The disk rename staging → <base>.{log,index} is
    // performed OFF the hot path by the background thread (Log enqueues it), which
    // then hands the final logPath_ back for the append thread to install.
    //
    // Crash-safety: if we crash after this in-memory adopt but before the bg
    // rename, the staging .log holds real (page-cache-or-fsync'd) data.  Log
    // recovery (recoverStagingFiles) reads the first batch header to recover the
    // true base and renames it into place — no data loss.
    baseOffset_ = base;
    nextOffset_ = base;
    durableNext_ = base;
    if (index_) index_->setBaseOffset(base);
    staging_ = false;
    pathPending_ = true;   // on-disk name is still the staging one until setLogPath
}

void LogSegment::setLogPath(const std::string& p) noexcept { logPath_ = p; pathPending_ = false; }

void LogSegment::setCanonicalLogPath(const std::string& dir, int64_t base) noexcept {
    char digits[24];
    std::snprintf(digits, sizeof digits, "%020lld", static_cast<long long>(base));
    logPath_.assign(dir);
    logPath_.push_back('/');
    logPath_.append(digits);
    logPath_.append(".log");
    pathPending_ = false;
}

LogSegment::LogSegment(std::string dir, int64_t baseOffset, int32_t indexIntervalBytes,
                       int32_t indexMaxBytes, bool prefaultIndex, bool eagerReadMap,
                       std::size_t readMapBytes, unsigned fileMode)
    : fileMode_(fileMode), baseOffset_(baseOffset), indexIntervalBytes_(indexIntervalBytes),
      indexMaxBytes_(indexMaxBytes), nextOffset_(baseOffset), durableNext_(baseOffset) {
    logPath_ = fname(dir, baseOffset, ".log");
    // D20: ONE raw POSIX fd is the segment's only handle on its .log — every
    // write (pwrite/pwritev), read (pread / read mmap), truncate (ftruncate) and
    // sync (fsync) goes through it.  No std::fstream: each one held a libc FILE*
    // (+ its own fd) for the segment's whole life, and glibc's fclose unlinks the
    // FILE from ONE process-wide list with a linear walk — O(open segments) per
    // segment destruction (>1M owner passes to drain 256 retention drops at 40k
    // segments).
    fd_ = ::open(logPath_.c_str(), O_RDWR | O_APPEND | O_CREAT | O_CLOEXEC, static_cast<mode_t>(fileMode));
    // F3 fix-round-1, item 2: see prepareStaging()'s identical check above —
    // an unchecked open() failure used to leave fd_ at -1 and silently
    // continue, so sync() would silently skip the fdatasync forever (a
    // silent durability loss).  Throw instead, exactly like OffsetIndex's and
    // MappedFile's construction failures already do.  Call sites that
    // construct a LogSegment (Log's ctor/installNextSegment/truncateTo/
    // reloadSegments) all construct into a LOCAL first and only install into
    // segments_ on success, so a throw here never leaves Log without an
    // active segment — see those call sites' comments.
    if (fd_ < 0) {
        throw std::system_error(errno, std::system_category(),
            "LogSegment: open failed for " + logPath_);
    }
    // Right-sized .index mmap (was a fixed 8 MiB / ~64x over-provision).  A full
    // rollBytes segment at indexIntervalBytes interval needs only a few KiB of
    // real entries; OffsetIndex grows the mapping on demand if a segment ever
    // exceeds indexMaxBytes_, so the smaller size is always safe.  prefaultIndex
    // (set by the background pre-provisioner) touches the pages up front so the
    // append/roll hot path never faults them.
    index_ = std::make_unique<OffsetIndex>(fname(dir, baseOffset, ".index"), baseOffset,
                                           indexMaxBytes_, prefaultIndex, fileMode);
    // F3: only the segment that IS or WILL BE active gets an eager read mapping
    // (+ its own read-only fd, though that fd is now closed right after mmap —
    // see MappedFile).  A sealed segment recovered from disk (eagerReadMap ==
    // false, the default) maps lazily on its first readView(): N retained
    // segments no longer means N eager read mmaps kept forever.
    if (eagerReadMap) openReadMap(readMapBytes);
}

// D3b fix round 2: the read mapping is ALWAYS made through this segment's own
// open fd_ (its inode, held for the segment's lifetime anyway) — never by
// re-opening logPath_.  A compaction install renames a rewritten file over the
// path (owner thread, fix round 2/3) and the legacy synchronous path replaces it
// before the reload; until the reload, this segment must keep reading ITS bytes: a
// by-path (re)map would expose the rewritten file (a marker skipped → a txn left
// open) or SIGBUS past its shorter EOF.  No extra fd (F3 unchanged).
std::unique_ptr<MappedFile> LogSegment::makeReadMap_(std::size_t bytes) {
    if (readMapFaultForTest_) [[unlikely]] throw std::runtime_error("LogSegment: injected read-map failure");
    if (fd_ >= 0) return std::make_unique<MappedFile>(MappedFile::BorrowedFd{fd_}, logPath_, bytes);
    return std::make_unique<MappedFile>(logPath_, bytes, /*writable*/false);   // unreachable: ctor throws on fd_ < 0
}

std::size_t LogSegment::readMapBytesFor(int64_t rollBytes) noexcept {
    const std::size_t want = rollBytes > 0 ? static_cast<std::size_t>(rollBytes) + kReadMapSlackBytes : kReadMapMinBytes;
    const std::size_t page = 4096;
    return std::max<std::size_t>(kReadMapMinBytes, (want + page - 1) / page * page);
}

std::unique_ptr<MappedFile> LogSegment::swapReadMap(std::unique_ptr<MappedFile> m) noexcept {
    std::unique_ptr<MappedFile> old = std::move(readMap_);
    // D20 review L5: a map built off the owner borrowed a dup the provisioner
    // has already closed — remap only ever through THIS segment's fd; the
    // retired map keeps no fd (its fd number may be reused after the segment).
    if (m) m->rebindBorrowedFd(fd_);
    if (old) old->rebindBorrowedFd(-1);
    readMap_ = std::move(m);
    readMapSize_ = readMap_ ? readMap_->size() : 0;
    return old;
}

void LogSegment::openReadMap(std::size_t bytes) noexcept {
    // Create the read-only .log mapping WITH the segment (spec §1a.5): the first
    // readView() after a roll must not allocate a MappedFile + mmap on the
    // push/fetch hot path.  kReadMapMinBytes of headroom past EOF (never touched:
    // readView reads only [0, logSize_)).  Failure is non-fatal — ensureReadMap()
    // falls back to creating it lazily.
    try {
        readMap_     = makeReadMap_(std::max(bytes, kReadMapMinBytes));
        readMapSize_ = readMap_->size();
    } catch (...) {
        readMap_.reset();
        readMapSize_ = 0;
    }
}

LogSegment::~LogSegment() {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
}

// D20: write all of [data, data+len) at the O_APPEND end of the .log through
// fd_ — the durable append() path (formerly a buffered fstream write + flush).
// Retries a short write / EINTR like the libc stream did; 0 or errno (ENOSPC for
// a zero-progress write).
int LogSegment::writeAll_(const uint8_t* data, std::size_t len) noexcept {
    const struct ::iovec one{const_cast<uint8_t*>(data), len};
    return finishShortWrite_(&one, 1, len, kNotWrittenYet, 0);
}

int LogSegment::finishShortWrite_(const struct ::iovec* iov, int cnt, std::size_t total, ssize_t first,
                                  int firstErr) noexcept {
    // D23 (D20 review L5): the first attempt's outcome is explicit — no errno
    // primed as a sentinel.  kNotWrittenYet: nothing attempted (writeAll_).
    std::size_t done = 0;
    if (first == kNotWrittenYet) {
        // start at 0
    } else if (first < 0) {
        if (firstErr != EINTR) return firstErr != 0 ? firstErr : EIO;
    } else if (first == 0 && total > 0) {
        return ENOSPC;
    } else {
        done = static_cast<std::size_t>(first);
    }
    std::size_t skip = done;
    for (int i = 0; i < cnt; ++i) {
        const auto* p = static_cast<const uint8_t*>(iov[i].iov_base);
        std::size_t n = iov[i].iov_len;
        if (skip >= n) { skip -= n; continue; }
        p += skip;
        n -= skip;
        skip = 0;
        while (n > 0) {
            const off_t off = static_cast<off_t>(logSize_) + static_cast<off_t>(done);
            const ssize_t w = (storage_ && storage_->pwriteForTest)
                ? storage_->pwriteForTest(fd_, p, n, off)
                : ::pwrite(fd_, p, n, off);
            if (w < 0) {
                if (errno == EINTR) continue;
                return errno != 0 ? errno : EIO;
            }
            if (w == 0) return ENOSPC;
            p += w;
            n -= static_cast<std::size_t>(w);
            done += static_cast<std::size_t>(w);
        }
    }
    return 0;
}

void LogSegment::sync() {
    if (storage_) {
        ++storage_->syncCalls;
        if (storage_->failedErrno != 0) {
            // fsyncgate: never retry a sync on a failed log — a later success
            // cannot vouch for dirty pages the kernel already dropped.
            throw std::system_error(storage_->failedErrno, std::generic_category(),
                                    "LogSegment::sync on a failed log " + logPath_);
        }
    }
    int err = 0;
    // 1. (D20) No libc buffer: every write already reached the page cache.
    if (storage_ && storage_->faultForTest != 0) err = storage_->faultForTest;
    // 2. fsync: flush page cache → storage device (data + size metadata needed to read).
    if (err == 0 && fd_ >= 0 && ::fsync(fd_) != 0) err = errno;
    // 3. msync: ensure mmap'd index pages are also written to device.
    if (err == 0) err = index_->sync();
    if (err != 0) {
        if (storage_ && storage_->failedErrno == 0) storage_->failedErrno = err;
        throw std::system_error(err, std::generic_category(), "LogSegment::sync " + logPath_);
    }
    syncedBytes_ = logSize_;
    durableNext_ = nextOffset_;   // E1d: everything written so far is on disk
}

int LogSegment::syncForRecovery() noexcept {
    if (storage_) {
        ++storage_->roleChangeSyncs;
        if (storage_->failedErrno != 0) return storage_->failedErrno;
    }
    int err = 0;
    if (storage_ && storage_->faultForTest != 0) err = storage_->faultForTest;
    if (err == 0 && fd_ >= 0 && ::fsync(fd_) != 0) err = errno != 0 ? errno : EIO;
    if (err != 0) {
        if (storage_ && storage_->failedErrno == 0) storage_->failedErrno = err;
        return err;
    }
    syncedBytes_ = logSize_;
    durableNext_ = nextOffset_;
    return 0;
}

void LogSegment::preallocate(int64_t bytes) noexcept {
    // #5 non-blocking append: reserve disk blocks up front so O_APPEND pwrite()s
    // never block on block allocation.  Keep the file SIZE unchanged (0 for a fresh
    // segment) so O_APPEND still writes at the true end-of-data — we only pre-reserve
    // the extents.  Best-effort: any failure (unsupported fs, ENOSPC) is a no-op; the
    // append path still works, just without the preallocation benefit.
    if (fd_ < 0 || bytes <= 0) return;
#if defined(__linux__)
    // FALLOC_FL_KEEP_SIZE: allocate blocks in [0, bytes) but do NOT grow st_size.
    (void)::fallocate(fd_, FALLOC_FL_KEEP_SIZE, 0, static_cast<off_t>(bytes));
#elif defined(__APPLE__)
    // F_PREALLOCATE reserves blocks past the current EOF without changing st_size.
    fstore_t fst{};
    fst.fst_flags   = F_ALLOCATECONTIG;      // try contiguous first
    fst.fst_posmode = F_PEOFPOSMODE;         // allocate `bytes` beyond current EOF
    fst.fst_offset  = 0;
    fst.fst_length  = static_cast<off_t>(bytes);
    if (::fcntl(fd_, F_PREALLOCATE, &fst) == -1) {
        fst.fst_flags = F_ALLOCATEALL;       // fall back to non-contiguous
        (void)::fcntl(fd_, F_PREALLOCATE, &fst);
    }
#else
    (void)bytes;
#endif
}

// ---------------------------------------------------------------------------
// writeAndUpdateState — shared core: update index + write bytes + advance LEO.
// Writes through fd_ (D20: pwrite, no fstream) for the durable append() path.
// Does NOT call sync(). Called by append() and (indirectly) appendNoSync().
// ---------------------------------------------------------------------------
int64_t LogSegment::writeAndUpdateState(const uint8_t* data, std::size_t len, int64_t lastOffset) {
    // Guard: batch must be at least the size of record-batch overhead.
    if (len < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead))
        throw std::runtime_error("rune: batch smaller than record-batch overhead");

    if (shouldIndex(bytesSinceIndex_, indexIntervalBytes_)) {
        rune::protocol::RecordBatchReader r(data, len);
        index_->append(r.baseOffset(), logSize_); // C4: OffsetIndex::append guards 2GiB
        bytesSinceIndex_ = 0;
    }
    if (storage_ && storage_->writeFaultForTest > 0) [[unlikely]]
        failWrite_(storage_->writeFaultForTest, "LogSegment::append write");
    if (const int e = writeAll_(data, len); e != 0) [[unlikely]]
        failWrite_(e, "LogSegment::append write");
    logSize_ += static_cast<int64_t>(len);
    bytesSinceIndex_ += static_cast<int64_t>(len);
    nextOffset_ = lastOffset + 1;
    return nextOffset_;
}

// E1 (DUR-1): a data-write error is fail-stop exactly like an fsync error (Kafka: an IOException on a
// log-dir append takes the dir offline -> halt).  With periodic fsync off this is how a dead disk is seen.
// Sticky (first error wins); the owner's loop routes storageFailed() to the broker halt.  A partial
// O_APPEND write may have left torn bytes: restart recovery truncates them.
void LogSegment::failWrite_(int err, const char* what) {
    if (err == 0) err = EIO;
    if (storage_ && storage_->failedErrno == 0) storage_->failedErrno = err;
    throw std::system_error(err, std::generic_category(), std::string("rune: ") + what + " failed (" + logPath_ + ")");
}

int64_t LogSegment::append(const uint8_t* data, std::size_t len, int64_t lastOffset) {
    int64_t next = writeAndUpdateState(data, len, lastOffset);
    sync();  // B1+B2: durable-by-default — fsync .log + msync .index after every append
    return next;
}
int64_t LogSegment::append(const std::vector<uint8_t>& batch, int64_t lastOffset) {
    // Vector overload: delegate to the zero-alloc pointer overload.
    return append(batch.data(), batch.size(), lastOffset);
}

int64_t LogSegment::appendNoSync(const uint8_t* data, std::size_t len, int64_t lastOffset) {
    // No-sync path: write via pwrite() directly to fd_ (O_RDWR|O_APPEND),
    // pwrite() goes directly to the OS page cache — no userspace buffer.
    // No fsync: data is page-cache-durable only (acks=Leader / acks=1 trade-off).
    //
    // Guard: batch must be at least the size of record-batch overhead.
    if (len < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead))
        throw std::runtime_error("rune: batch smaller than record-batch overhead");

    // Update the sparse index (mmap write, no syscall).
    if (shouldIndex(bytesSinceIndex_, indexIntervalBytes_)) {
        rune::protocol::RecordBatchReader r(data, len);
        index_->append(r.baseOffset(), logSize_); // C4: OffsetIndex::append guards 2GiB
        bytesSinceIndex_ = 0;
    }

    // Write directly to the fd with O_APPEND semantics: pwrite() at offset
    // logSize_ is equivalent to an append for an O_APPEND fd.  We use
    // logSize_ as the explicit offset so the write lands at the correct position.
    // pwrite() on macOS with O_APPEND ignores the offset argument for the actual
    // write location (always appends to end), but using logSize_ makes the intent
    // clear and lets pread() read the data from the same position.
    // (D20: append() writes through the same fd_ — the paths may mix freely.)
    //
    // Durability: pwrite() to a non-O_SYNC fd writes to page cache only.
    // A leader crash before sync() (flushDirtyLogs) may lose the tail.
    // TODO(Milestone C): acks=All waits for ISR HWM before acking.
    // NOTE: the sparse-index entry above is written BEFORE the data.  If the write below fails the
    // entry points at bytes never written — harmless: the log is then failed (no reads/appends) and
    // restart recovery rebuilds the index from the valid data.
    off_t writePos = static_cast<off_t>(logSize_);
    ssize_t written;
    if (storage_ && storage_->writeFaultForTest != 0) [[unlikely]] {
        if (storage_->writeFaultForTest != LogStorageState::kShortWriteOnceForTest)
            failWrite_(storage_->writeFaultForTest, "LogSegment::appendNoSync pwrite");
        storage_->writeFaultForTest = 0;
        written = ::pwrite(fd_, data, len / 2, writePos);   // a simulated short write
    } else {
        written = ::pwrite(fd_, data, len, writePos);
    }
    if (written != static_cast<ssize_t>(len)) [[unlikely]] {
        // D20 review L2: resumed like the durable path (EINTR / short write).
        const int firstErr = written < 0 ? errno : 0;
        const struct ::iovec one{const_cast<uint8_t*>(data), len};
        if (const int e = finishShortWrite_(&one, 1, len, written, firstErr); e != 0)
            failWrite_(e, "LogSegment::appendNoSync pwrite");
    }

    logSize_ += static_cast<int64_t>(len);
    bytesSinceIndex_ += static_cast<int64_t>(len);
    nextOffset_ = lastOffset + 1;
    return nextOffset_;
}

int64_t LogSegment::appendNoSyncRestamp(const uint8_t* data, std::size_t len,
                                        int64_t baseOffset, int64_t lastOffset,
                                        int32_t leaderEpoch) {
    // Zero-copy restamp: identical page-cache write to appendNoSync(), but the
    // base-offset field [0..7] is patched to the ASSIGNED offset via pwritev()
    // scatter-gather instead of mutating `data`.  `data` stays read-only (it may
    // point straight at an Aeron fragment / ring slot), so NO restampScratch_.
    static_assert(rune::protocol::batch::kBaseOffsetOffset == 0,
                  "restamp iovec split assumes base-offset field starts at byte 0");
    if (len < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead))
        throw std::runtime_error("rune: batch smaller than record-batch overhead");

    // Sparse index: use the ASSIGNED baseOffset (data still carries the stale
    // client value, so we must NOT read it via RecordBatchReader::baseOffset()).
    if (shouldIndex(bytesSinceIndex_, indexIntervalBytes_)) {
        index_->append(baseOffset, logSize_); // C4: OffsetIndex guards 2GiB
        bytesSinceIndex_ = 0;
    }

    // pwritev: iov[0] = 8-byte LE64 assigned base offset, iov[1] = the read-only
    // fragment tail [8 .. len).  Single syscall, zero mutable copy of the batch.
    uint8_t leBase[8];
    {
        uint64_t u = static_cast<uint64_t>(baseOffset);
        for (int i = 0; i < 8; ++i) leBase[i] = static_cast<uint8_t>(u >> (i * 8));
    }
    // Leader-epoch stamp (Kafka parity): iov = [base 8][length 4][epoch 4][tail].
    static_assert(rune::protocol::batch::kPartitionLeaderEpochOffset == 12,
                  "restamp iovec split assumes the epoch field at bytes [12..15]");
    uint8_t leEpoch[4];
    {
        const uint32_t u = static_cast<uint32_t>(leaderEpoch);
        for (int i = 0; i < 4; ++i) leEpoch[i] = static_cast<uint8_t>(u >> (i * 8));
    }
    struct iovec iov[4];
    int iovCnt = 2;
    iov[0].iov_base = leBase;
    iov[0].iov_len  = 8;
    if (leaderEpoch >= 0) {
        iov[1].iov_base = const_cast<uint8_t*>(data + 8);
        iov[1].iov_len  = 4;
        iov[2].iov_base = leEpoch;
        iov[2].iov_len  = 4;
        iov[3].iov_base = const_cast<uint8_t*>(data + 16);
        iov[3].iov_len  = len - 16;
        iovCnt = 4;
    } else {
        iov[1].iov_base = const_cast<uint8_t*>(data + 8);
        iov[1].iov_len  = len - 8;
    }

    off_t writePos = static_cast<off_t>(logSize_);
    ssize_t written;
    if (storage_ && storage_->writeFaultForTest != 0) [[unlikely]] {
        if (storage_->writeFaultForTest != LogStorageState::kShortWriteOnceForTest)
            failWrite_(storage_->writeFaultForTest, "LogSegment::appendNoSyncRestamp pwritev");
        storage_->writeFaultForTest = 0;
        written = ::pwritev(fd_, iov, 1, writePos);   // a simulated short write (first iovec only)
    } else {
        written = ::pwritev(fd_, iov, iovCnt, writePos);
    }
    if (written != static_cast<ssize_t>(len)) [[unlikely]] {
        // D20 review L2: resumed like the durable path (EINTR / short write).
        const int firstErr = written < 0 ? errno : 0;
        if (const int e = finishShortWrite_(iov, iovCnt, len, written, firstErr); e != 0)
            failWrite_(e, "LogSegment::appendNoSyncRestamp pwritev");
    }

    // maxTimestamp lives at kMaxTimestampOffset (outside the base-offset field),
    // so reading it from the read-only `data` is correct.
    if (len >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
        rune::protocol::RecordBatchReader r(data, len);
        int64_t ts = r.maxTimestamp();
        (void)ts; // segment-level maxTs tracked in Log::appendNoSyncRestamp
    }

    logSize_ += static_cast<int64_t>(len);
    bytesSinceIndex_ += static_cast<int64_t>(len);
    nextOffset_ = lastOffset + 1;
    return nextOffset_;
}

std::vector<uint8_t> LogSegment::read(int64_t startOffset, int32_t maxBytes) {
    OffsetPosition op = index_->lookup(startOffset);
    int64_t remaining = logSize_ - op.position;
    // Guard: clamp negative remaining (if index is out of sync) to prevent huge allocation
    if (remaining < 0) remaining = 0;
    int64_t toRead = std::min<int64_t>(remaining, maxBytes);
    // Grow-and-retry loop: the sparse index may point up to indexIntervalBytes_ before
    // startOffset's batch. If maxBytes is smaller than that skip distance the initial
    // window is too small to reach the target — double the window until we either find
    // the batch or exhaust the segment.
    for (;;) {
        std::vector<uint8_t> buf(static_cast<size_t>(toRead));
        // pread() via fd_ (O_RDWR | O_APPEND; pread ignores the file position).
        if (toRead > 0 && fd_ >= 0) {
            ssize_t nread = ::pread(fd_, buf.data(), static_cast<std::size_t>(toRead),
                                   static_cast<off_t>(op.position));
            if (nread < 0) nread = 0;
            buf.resize(static_cast<size_t>(nread));
        }
        // Skip batches whose lastOffset < startOffset (index is sparse -> may start earlier).
        // Guard: require at least kRecordBatchOverhead bytes before constructing a reader.
        size_t pos = 0;
        while (pos + static_cast<size_t>(rune::protocol::batch::kRecordBatchOverhead) <= buf.size()) {
            rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
            int32_t total = r.totalLength();
            // Batch cut off by window boundary — stop; outer loop will grow the window.
            if (total < rune::protocol::batch::kRecordBatchOverhead || pos + static_cast<size_t>(total) > buf.size()) break;
            int64_t batchLast = r.baseOffset() + r.lastOffsetDelta();
            if (batchLast >= startOffset) {
                return std::vector<uint8_t>(buf.begin() + static_cast<ptrdiff_t>(pos), buf.end());
            }
            pos += static_cast<size_t>(total);
        }
        // If we already read the entire segment, the offset is genuinely absent.
        if (toRead >= remaining) return {};
        // Grow: at minimum jump by one full index interval + overhead so we always make progress.
        int64_t minGrow = toRead + static_cast<int64_t>(indexIntervalBytes_) +
                          static_cast<int64_t>(rune::protocol::batch::kRecordBatchOverhead);
        toRead = std::min(remaining, std::max(toRead * 2, minGrow));
    }
}

bool LogSegment::ensureReadMap() noexcept {
    if (logSize_ <= 0) return true;                   // mmap(len=0) is EINVAL — nothing to map yet
    const std::size_t want = static_cast<std::size_t>(logSize_);
    if (readMap_ && readMapSize_ >= want) return true; // steady state: no syscall
    if (readMap_ && !ownerRemap_) return true;        // review Md5: grown off-thread — serve the mapped prefix
    // Map with GEOMETRIC HEADROOM past EOF (read-only, so the file is never
    // extended; the pages beyond EOF are never touched — readView reads only
    // [0, logSize_)).  The active segment then grows inside one mapping instead of
    // munmap+mmap on every append: views keep a stable address and a push/fetch
    // costs no syscalls.  A remap happens only O(log(segment/kMin)) times.
    std::size_t cap = readMapSize_ > 0 ? readMapSize_ : kReadMapMinBytes;
    while (cap < want) cap *= 2;
    ++readMapRemaps_;
    try {
        if (!readMap_) {
            // Read-only mapping (writable=false → no ftruncate; never mutates .log).
            readMap_ = makeReadMap_(cap);
        } else {
            // For a read-only MappedFile, resize() munmaps + remaps WITHOUT ftruncate
            // — through this segment's own fd (D3b r2), never by path.
            readMap_->resize(cap);
        }
        readMapSize_ = readMap_->size();
        // readMapSize_ > logSize_ (headroom / post-truncate) is memory-safe: readView()
        // only accesses bytes in [0, logSize_), which are backed by the file.
        return true;
    } catch (...) {
        // F3: readView() must never throw.  A map failure (missing file, ENOMEM,
        // max_map_count, ...) leaves readMap_/readMapSize_ null/0 — never a
        // half-constructed or stale mapping — and readView() returns empty this
        // cycle; callers already treat empty as "nothing now, retry next cycle".
        readMap_.reset();
        readMapSize_ = 0;
        ++readMapFailures_;
        if (readMapFailures_ == 1 || readMapFailures_ % 1024 == 0) {
            spdlog::warn("LogSegment: read-map failed for {} (failures={})",
                         logPath_, readMapFailures_);
        }
        return false;
    }
}

LogReadView LogSegment::mappedBytes() noexcept {
    if (logSize_ <= 0) return {};
    ensureReadMap();
    if (!readMap_) return {};
    LogReadView v;
    v.data       = readMap_->data();
    v.len        = std::min(static_cast<std::size_t>(logSize_), readMapSize_);
    v.baseOffset = baseOffset_;
    v.nextOffset = nextOffset_;
    return v;
}

LogReadView LogSegment::readInto(int64_t startOffset, int32_t maxBytes, uint8_t* buf, std::size_t bufCap,
                                 bool& firstTooLarge) noexcept {
    ReadIntoStatus st;
    const LogReadView v = readInto(startOffset, maxBytes, buf, bufCap, st);
    firstTooLarge = st == ReadIntoStatus::FirstTooLarge;
    return v;
}

LogReadView LogSegment::readInto(int64_t startOffset, int32_t maxBytes, uint8_t* buf, std::size_t bufCap,
                                 ReadIntoStatus& st, int64_t boundOffset, int* errOut) noexcept {
    st = ReadIntoStatus::Absent;
    if (logSize_ <= 0) return {};                                     // nothing written: absent
    if (fd_ < 0 || buf == nullptr || bufCap == 0) { st = ReadIntoStatus::Unreadable; return {}; }
    // Lows A/B/C review M1: a failed log is never pread again (a bad sector
    // re-read on every worker pass blocks the duty pass for the drive's retry
    // time) — IoError, at once.
    if (storage_ != nullptr && storage_->failedErrno != 0) [[unlikely]] {
        st = ReadIntoStatus::IoError;
        if (errOut != nullptr) *errOut = storage_->failedErrno;
        return {};
    }
    using rune::protocol::RecordBatchReader;
    const std::size_t kOverhead = static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
    const std::size_t interval = static_cast<std::size_t>(indexIntervalBytes_);
    // The sparse index points at or before the target batch: less than one
    // index interval earlier for a present offset; up to one interval + the
    // batch before a compaction gap's first survivor.  One window covers that
    // skip + maxBytes.
    const OffsetPosition op = index_->lookup(startOffset);
    const int64_t pos0 = op.position > 0 ? op.position : 0;
    if (pos0 >= logSize_) return {};
    const std::size_t remaining = static_cast<std::size_t>(logSize_ - pos0);
    // Only what this read needs: maxBytes + the sparse-index skip (<= two index
    // intervals) — never the whole buffer.  A first batch past it -> read on.
    std::size_t need = (maxBytes > 0 ? static_cast<std::size_t>(maxBytes) : 0) + 2 * interval + kOverhead;
    // D26 (fix65 review LOW-4): a caller that delivers nothing at/after
    // `boundOffset` (the HW / LSO) needs no bytes past the batch holding it —
    // that batch starts less than one index interval after the index entry
    // for the bound.  (The first batch is still read whole: the scan reads on.)
    if (boundOffset < nextOffset_ && boundOffset > startOffset) {
        const int64_t pb = index_->lookup(boundOffset).position;
        if (pb >= pos0) need = std::min(need, static_cast<std::size_t>(pb - pos0) + interval + kOverhead);
    }
    const std::size_t want = std::min({remaining, bufCap, need});
    const std::size_t limit = std::min(remaining, bufCap);   // the most this buffer can ever hold
    std::size_t got = 0;
    int ioErr = 0;   // D26 (fix65 review LOW-2): sticky — an I/O error is never reported as "too large"
    // pread [got, upTo) (short reads / EINTR retried); false on error / EOF before upTo.
    auto fill = [&](std::size_t upTo) noexcept {
        while (got < upTo) {
            const ssize_t r = ::pread(fd_, buf + got, upTo - got, static_cast<off_t>(pos0) + static_cast<off_t>(got));
            if (r < 0) {
                if (errno == EINTR) continue;
                ioErr = errno != 0 ? errno : EIO;
                return false;
            }
            if (r == 0) { ioErr = EIO; return false; }   // shorter than its written size: a mapping would SIGBUS
            got += static_cast<std::size_t>(r);
        }
        return true;
    };
    // Lows A/B/C review M1: a read I/O error fails the log like a write /
    // fsync error (sticky failedErrno; first error wins).  The owner's
    // checkStorageFailure_ routes it to the log-dir-failure path (D15
    // LogDirOffline / broker halt) — Kafka: a read IOException takes the log
    // dir offline.  Never an empty answer retried on every pass.
    auto failed = [&]() noexcept -> LogReadView {
        st = ReadIntoStatus::IoError;
        ++readIntoErrors_;
        if (storage_ != nullptr && storage_->failedErrno == 0) storage_->failedErrno = ioErr != 0 ? ioErr : EIO;
        if (errOut != nullptr) *errOut = ioErr;
        return {};
    };
    // A first batch too large for `buf` (rare: the caller may then serve it
    // through the read mapping).  Its bytes are read through once (into `buf`,
    // discarded) first: a bad / missing page answers IoError here instead of a
    // SIGBUS in the mapped fallback (D26, fix65 review LOW-2).  Cold.  Lows
    // A/B/C review L6: once per batch — a batch read through cleanly is
    // remembered (position + length; forgotten at a truncate / recover) and a
    // retry goes straight to the fallback (no second full read per attempt).
    auto tooLarge = [&](std::size_t at, std::size_t len) noexcept -> LogReadView {
        const int64_t abs = pos0 + static_cast<int64_t>(at);
        if (abs == tooLargeOkPos_ && len == tooLargeOkLen_) { st = ReadIntoStatus::FirstTooLarge; return {}; }
        for (std::size_t off = 0; off < len;) {
            const std::size_t n = std::min(bufCap, len - off);
            const ssize_t r = ::pread(fd_, buf, n, static_cast<off_t>(pos0) + static_cast<off_t>(at + off));
            if (r < 0) {
                if (errno == EINTR) continue;
                ioErr = errno != 0 ? errno : EIO;
                return failed();
            }
            if (r == 0) { ioErr = EIO; return failed(); }
            off += static_cast<std::size_t>(r);
            readIntoBytes_ += static_cast<uint64_t>(r);
        }
        tooLargeOkPos_ = abs;
        tooLargeOkLen_ = len;
        st = ReadIntoStatus::FirstTooLarge;
        return {};
    };
    if (!fill(want)) return failed();
    readIntoBytes_ += got;
    std::size_t pos = 0, firstPos = SIZE_MAX;
    int64_t firstBase = 0;
    for (;;) {
        if (pos + kOverhead > got) {
            // Window ended between batches: read on (up to the buffer) unless
            // the segment's bytes are exhausted (the offset is absent).
            if (got >= remaining) return {};
            if (got >= limit) {
                // Not even this batch's header fits after the skip: read it
                // aside to learn the batch's extent (cold).
                uint8_t hdr[rune::protocol::batch::kRecordBatchOverhead];
                std::size_t h = 0;
                while (h < kOverhead) {
                    const ssize_t r = ::pread(fd_, hdr + h, kOverhead - h,
                                              static_cast<off_t>(pos0) + static_cast<off_t>(pos + h));
                    if (r < 0 && errno == EINTR) continue;
                    if (r <= 0) { ioErr = r < 0 && errno != 0 ? errno : EIO; return failed(); }
                    h += static_cast<std::size_t>(r);
                }
                const int32_t total = RecordBatchReader(hdr, kOverhead).totalLength();
                if (total < rune::protocol::batch::kRecordBatchOverhead || pos + static_cast<std::size_t>(total) > remaining) {
                    st = ReadIntoStatus::Unreadable;
                    return {};
                }
                return tooLarge(pos, static_cast<std::size_t>(total));
            }
            const std::size_t before = got;
            if (!fill(std::min(limit, pos + kOverhead))) return failed();
            readIntoBytes_ += got - before;
            continue;
        }
        RecordBatchReader r(buf + pos, got - pos);
        const int32_t total = r.totalLength();
        if (total < rune::protocol::batch::kRecordBatchOverhead) { st = ReadIntoStatus::Unreadable; return {}; }   // torn / corrupt
        const std::size_t end = pos + static_cast<std::size_t>(total);
        if (end > got) {
            // Cut by the request-sized window: read the rest of THIS batch when
            // the buffer can hold it (review MEDIUM-1: never a needless fallback).
            if (end > remaining) { st = ReadIntoStatus::Unreadable; return {}; }   // past the written end: torn
            if (end > limit) return tooLarge(pos, end - pos);
            const std::size_t before = got;
            if (!fill(end)) return failed();
            readIntoBytes_ += got - before;
            continue;
        }
        if (r.baseOffset() + r.lastOffsetDelta() >= startOffset) { firstPos = pos; firstBase = r.baseOffset(); break; }
        pos = end;
    }
    const std::size_t cap = maxBytes > 0 ? static_cast<std::size_t>(maxBytes) : 0;
    std::size_t end = firstPos;
    int64_t nextOff = firstBase;
    while (end + kOverhead <= got) {
        RecordBatchReader r(buf + end, got - end);
        const int32_t total = r.totalLength();
        if (total < rune::protocol::batch::kRecordBatchOverhead || end + static_cast<std::size_t>(total) > got) break;
        if (end != firstPos) {
            if (cap == 0) break;
            if ((end - firstPos) + static_cast<std::size_t>(total) > cap) break;
        }
        end += static_cast<std::size_t>(total);
        nextOff = r.baseOffset() + r.lastOffsetDelta() + 1;
    }
    st = ReadIntoStatus::Ok;
    LogReadView v;
    v.data       = buf + firstPos;
    v.len        = end - firstPos;
    v.baseOffset = firstBase;
    v.nextOffset = nextOff;
    return v;
}

LogReadView LogSegment::readView(int64_t startOffset, int32_t maxBytes) noexcept {
    if (logSize_ <= 0) return {};
    ensureReadMap();
    const uint8_t* base = readMap_ ? readMap_->data() : nullptr;
    // Never read past the mapping: bound by the size the map was built for.
    const std::size_t size =
        std::min(static_cast<std::size_t>(logSize_), readMapSize_);
    if (!base || size == 0) return {};

    using rune::protocol::RecordBatchReader;
    const std::size_t kOverhead =
        static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);

    // Locate the first batch whose lastOffset >= startOffset, scanning forward
    // from the sparse-index position (which points at or before the target batch).
    OffsetPosition op = index_->lookup(startOffset);
    std::size_t pos = op.position > 0 ? static_cast<std::size_t>(op.position) : 0;
    if (pos > size) pos = size;
    std::size_t firstPos = size;   // sentinel: not found
    int64_t firstBase = 0;
    while (pos + kOverhead <= size) {
        RecordBatchReader r(base + pos, size - pos);
        int32_t total = r.totalLength();
        // Torn / not-yet-fully-written tail — stop (readView never exposes it).
        if (total < rune::protocol::batch::kRecordBatchOverhead || pos + static_cast<std::size_t>(total) > size) break;
        const int64_t batchLast = r.baseOffset() + r.lastOffsetDelta();
        if (batchLast >= startOffset) { firstPos = pos; firstBase = r.baseOffset(); break; }
        pos += static_cast<std::size_t>(total);
    }
    if (firstPos == size) return {};   // offset not present in this segment

    // Accumulate WHOLE batches from firstPos up to maxBytes.  Always include the
    // first batch even if it alone exceeds maxBytes (progress guarantee).  A
    // non-positive maxBytes means "one batch only" (minimal progress), NOT
    // "unlimited": callers pass a positive chunk cap; 0/negative is a degenerate
    // request that must still make bounded forward progress.
    const std::size_t cap = maxBytes > 0 ? static_cast<std::size_t>(maxBytes) : 0;
    std::size_t end = firstPos;
    int64_t nextOff = firstBase;
    while (end + kOverhead <= size) {
        RecordBatchReader r(base + end, size - end);
        int32_t total = r.totalLength();
        if (total < rune::protocol::batch::kRecordBatchOverhead || end + static_cast<std::size_t>(total) > size) break;
        if (end != firstPos) {
            // Past the first batch: only take more if a positive cap has room.
            if (cap == 0) break;  // maxBytes<=0 → exactly one batch
            if ((end - firstPos) + static_cast<std::size_t>(total) > cap) break;
        }
        end += static_cast<std::size_t>(total);
        nextOff = r.baseOffset() + r.lastOffsetDelta() + 1;
    }

    LogReadView v;
    v.data       = base + firstPos;
    v.len        = end - firstPos;
    v.baseOffset = firstBase;
    v.nextOffset = nextOff;
    return v;
}

int LogSegment::openTrusted(int64_t nextOffset) noexcept {
    struct stat st{};
    if (::fstat(fd_, &st) != 0) return errno != 0 ? errno : EIO;
    logSize_     = static_cast<int64_t>(st.st_size);
    nextOffset_  = nextOffset;
    durableNext_ = nextOffset;
    syncedBytes_ = logSize_;
    index_->rebuildEntries();   // counts the cleaner-built entries (O(index entries), no I/O)
    return 0;
}

int LogSegment::truncateInPlace(int64_t truncateOffset, int64_t& keptMaxTs) noexcept {
    // Re-review N1: the cut is derived from the WHOLE segment — never from a
    // prefix-only mapping (Md5 grows maps off the owner; a demoted leader's
    // segment may never have had its grown map installed).  This cold path
    // maps the whole file on the owner if it must.
    const bool savedRemap = ownerRemap_;
    ownerRemap_ = true;
    const bool mapped = ensureReadMap() && readMap_ && readMapSize_ >= static_cast<std::size_t>(logSize_);
    ownerRemap_ = savedRemap;
    if (!mapped) return -1;
    const uint8_t* base = readMap_->data();
    const std::size_t size = static_cast<std::size_t>(logSize_);
    const std::size_t kOverhead = static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
    std::size_t pos = 0;
    int64_t next = baseOffset_;
    int64_t maxTs = 0;
    bool atCut = false;
    while (pos + kOverhead <= size) {
        rune::protocol::RecordBatchReader r(base + pos, size - pos);
        const int32_t total = r.totalLength();
        if (total < rune::protocol::batch::kRecordBatchOverhead || pos + static_cast<std::size_t>(total) > size) break;
        if (r.baseOffset() + r.lastOffsetDelta() >= truncateOffset) { atCut = true; break; }   // cut here (whole batch)
        const int64_t ts = toEpochMs(r.maxTimestamp());
        if (ts > maxTs) maxTs = ts;
        next = r.baseOffset() + r.lastOffsetDelta() + 1;
        pos += static_cast<std::size_t>(total);
    }
    // Re-review N1: the walk must stop AT the batch holding truncateOffset (or
    // at the true end of the segment's batches) — anything else (an unparsable
    // batch inside the segment) would cut below the requested offset and
    // silently delete records: fail the log loudly instead.
    if (!atCut && pos != size) [[unlikely]] return EIO;
    if (::ftruncate(fd_, static_cast<off_t>(pos)) != 0) return errno != 0 ? errno : EIO;
    const int64_t lastIdx = index_->truncateToPosition(static_cast<int64_t>(pos));
    logSize_ = static_cast<int64_t>(pos);
    tooLargeOkPos_ = -1;   // review L6: the bytes past the cut may be rewritten
    // Walked the whole segment without reaching truncateOffset (pos == size):
    // nothing is cut.  A compacted segment's logical end can lie past its last
    // surviving batch, so keep it, capped at the requested offset (re-review-2
    // medium: the former "next != nextOffset_" guard fail-stopped this no-op).
    nextOffset_ = atCut ? next : std::min(nextOffset_, truncateOffset);
    bytesSinceIndex_ = lastIdx < 0 ? 0 : logSize_ - lastIdx;
    if (syncedBytes_ > logSize_) syncedBytes_ = logSize_;
    if (durableNext_ > nextOffset_) durableNext_ = nextOffset_;
    if (durableNext_ < baseOffset_) durableNext_ = baseOffset_;
    keptMaxTs = maxTs;
    return 0;
}

int64_t LogSegment::recover(bool syncIndex) {
    // D20 review L3 — ONE rule for every error: it fails the log (sticky
    // failedErrno, routed to the halt by the owner's loop), and it THROWS only
    // while the segment's state is unknown (fstat / pread: no valid prefix
    // established — a caller must never act on it, e.g. delete "later"
    // segments on a bogus chain end).  Once the valid prefix is known
    // (ftruncate, index rename, index msync, directory fsync) recover()
    // completes the in-memory state and returns — Task 10 fix 3: no exception
    // escapes a worker's role-change truncation.  Without a storage state
    // (standalone segment) an error always throws (nothing else reports it).
    auto failSticky = [this](int e) {
        if (storage_ && storage_->failedErrno == 0) storage_->failedErrno = e != 0 ? e : EIO;
    };
    auto fail = [&](int e, const std::string& what) {
        if (e == 0) e = EIO;
        failSticky(e);
        throw std::system_error(e, std::system_category(), what);
    };
    // D20: read the whole .log through fd_ (pread), not a FILE*-backed stream.
    struct stat st{};
    if (::fstat(fd_, &st) != 0) fail(errno, "LogSegment::recover fstat " + logPath_);
    const int64_t fileSize = static_cast<int64_t>(st.st_size);
    std::vector<uint8_t> buf(static_cast<size_t>(fileSize));
    {
        std::size_t got = 0;
        while (got < buf.size()) {
            const ssize_t r = ::pread(fd_, buf.data() + got, buf.size() - got, static_cast<off_t>(got));
            if (r < 0) {
                if (errno == EINTR) continue;
                fail(errno, "LogSegment::recover pread " + logPath_);
            }
            if (r == 0) break;   // shrank under us: validate what was read
            got += static_cast<std::size_t>(r);
        }
        buf.resize(got);
    }

    // B3: rebuild into .index.tmp, then atomically rename over the canonical .index
    // so there is never a window where the on-disk index is stale/orphaned.
    // (The old code wrote to ".index2" and left the real ".index" permanently stale.)
    std::string indexBase = logPath_.substr(0, logPath_.size()-4);
    std::string indexPath = indexBase + ".index";
    std::string indexTmp  = indexBase + ".index.tmp";
    // Remove any leftover .tmp from a previous crash
    std::filesystem::remove(indexTmp);
    index_ = std::make_unique<OffsetIndex>(indexTmp, baseOffset_, indexMaxBytes_, false, fileMode_);

    size_t pos = 0; int64_t validBytes = 0; int64_t next = baseOffset_;
    // C2: seed bytesSinceIndex_ identically to append() so the index is reproducible.
    bytesSinceIndex_ = 0;
    using rune::protocol::RecordBatchReader;
    while (pos + rune::protocol::batch::kRecordBatchOverhead <= buf.size()) {
        RecordBatchReader r(buf.data() + pos, buf.size() - pos);
        int32_t total = r.totalLength();
        if (total < rune::protocol::batch::kRecordBatchOverhead || pos + static_cast<size_t>(total) > buf.size() || !r.ensureValid()) break;
        // C2: use the same shouldIndex() predicate as append()
        if (shouldIndex(bytesSinceIndex_, indexIntervalBytes_)) {
            index_->append(r.baseOffset(), validBytes); // C4: OffsetIndex::append guards 2GiB
            bytesSinceIndex_ = 0;
        }
        validBytes += total; bytesSinceIndex_ += total;
        next = r.baseOffset() + r.lastOffsetDelta() + 1;
        pos += total;
    }

    // Truncate torn tail.
    // D20: through the segment's own fd (its inode), never by path.
    if (::ftruncate(fd_, static_cast<off_t>(validBytes)) != 0) {
        // State known (validBytes / next); the torn tail stays on disk behind a
        // failed log (no append reaches it) and restart recovery cuts it.
        if (!storage_) fail(errno, "LogSegment::recover ftruncate " + logPath_);
        failSticky(errno);
    }
    logSize_ = validBytes; nextOffset_ = next;
    tooLargeOkPos_ = -1;   // review L6
    if (syncedBytes_ > validBytes) syncedBytes_ = validBytes;   // E2 crash seam
    // E1d: recover() itself proves nothing durable — the owning Log decides
    // (trusted below the recovery point, fsynced in the validated region).
    if (durableNext_ > nextOffset_) durableNext_ = nextOffset_;
    if (durableNext_ < baseOffset_) durableNext_ = baseOffset_;

    // B3: msync the rebuilt index then atomically rename .index.tmp → .index,
    // then fsync the directory so the renamed entry survives power loss.
    // Accepted role-change sync work (truncateTo on a role / epoch change, and
    // startup recovery) — counted, and an error is fail-stop (sticky per log)
    // rather than an exception escaping a worker loop.
    if (syncIndex) {
        if (storage_) ++storage_->roleChangeSyncs;
        if (const int e = (storage_ && storage_->faultForTest) ? storage_->faultForTest : index_->sync(); e != 0) {
            if (storage_ && storage_->failedErrno == 0) storage_->failedErrno = e;
        }
    }
    if (::rename(indexTmp.c_str(), indexPath.c_str()) != 0) {
        // State known: the rebuilt in-memory index (on the .tmp file) stays valid.
        if (!storage_) fail(errno, "LogSegment::recover rename " + indexTmp + " -> " + indexPath);
        failSticky(errno);
        return nextOffset_;
    }
    // fsync the directory for durability of the rename
    if (syncIndex) {
        std::string dir = logPath_.substr(0, logPath_.rfind('/'));
        int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) {
            if (storage_) ++storage_->roleChangeSyncs;
            if (::fsync(dfd) != 0 && storage_ && storage_->failedErrno == 0) storage_->failedErrno = errno;
            ::close(dfd);
        }
    }
    // Reopen index_ from the canonical path and recount entries from mmap data.
    index_ = std::make_unique<OffsetIndex>(indexPath, baseOffset_, indexMaxBytes_, false, fileMode_);
    index_->rebuildEntries();

    return nextOffset_;
}
} // namespace rune::storage
