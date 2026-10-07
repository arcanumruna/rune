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
    seg->log_.open(logFile, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!seg->log_.is_open())
        throw std::runtime_error("LogSegment::openPrebuilt: cannot open " + logFile);
    seg->fd_ = ::open(logFile.c_str(), O_RDWR | O_APPEND);
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
    // Construct at provisional base 0.  All the expensive syscalls (2 open()s for
    // the .log fstream+fd, 1 open() + ftruncate + mmap + prefault for the .index)
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
    seg->log_.open(seg->logPath_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!seg->log_.is_open()) { std::ofstream(seg->logPath_, std::ios::binary).close();
        seg->log_.open(seg->logPath_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app); }
    seg->fd_ = ::open(seg->logPath_.c_str(), O_RDWR | O_APPEND | O_CREAT, 0644);
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
    seg->openReadMap(readMapBytes);   // background thread: the read mapping is ready at adopt — whole-segment sized (F5)
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
                       std::size_t readMapBytes)
    : baseOffset_(baseOffset), indexIntervalBytes_(indexIntervalBytes),
      indexMaxBytes_(indexMaxBytes), nextOffset_(baseOffset), durableNext_(baseOffset) {
    logPath_ = fname(dir, baseOffset, ".log");
    log_.open(logPath_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!log_.is_open()) { std::ofstream(logPath_, std::ios::binary).close();
        log_.open(logPath_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app); }
    // Open a POSIX fd for fsync and direct reads (pread).
    // O_RDWR so we can use pread() from it in read() without the fstream
    // mixed-mode slowdown on macOS.  Writes still go through fstream for
    // simplicity; fd_ is used for fdatasync and pread only.
    fd_ = ::open(logPath_.c_str(), O_RDWR | O_APPEND | O_CREAT, 0644);
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
                                           indexMaxBytes_, prefaultIndex);
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

int LogSegment::flushBufferedToPageCache() noexcept {
    log_.flush();
    return log_ ? 0 : EIO;
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
    // 1. Flush libc buffer → OS page cache.
    log_.flush();
    if (!log_) err = EIO;
    if (err == 0 && storage_ && storage_->faultForTest != 0) err = storage_->faultForTest;
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
    log_.flush();
    if (!log_) err = EIO;
    if (err == 0 && storage_ && storage_->faultForTest != 0) err = storage_->faultForTest;
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
// Writes via fstream (buffered, write-only path) for the durable append() path.
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
    log_.seekp(0, std::ios::end);
    if (storage_ && storage_->writeFaultForTest != 0) [[unlikely]] {
        log_.setstate(std::ios::badbit);
        failWrite_(storage_->writeFaultForTest, "LogSegment::append write");
    }
    log_.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(len));
    if (!log_) [[unlikely]] failWrite_(errno != 0 ? errno : EIO, "LogSegment::append write");
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
    // bypassing the fstream write path entirely.  pwrite() goes directly to the
    // OS page cache — no userspace buffer, no seekp/write fstream overhead.
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
    //
    // Note: because we bypass fstream's write buffer here, the fstream's internal
    // "dirty" state may be stale. We must NOT mix fstream writes and pwrite()
    // calls without a sync — so appendNoSync exclusively uses pwrite(), and
    // append() (durable path) uses fstream.  In practice, the log is only ever
    // used via one path (either fstream-append or pwrite-appendNoSync) at a time.
    //
    // Durability: pwrite() to a non-O_SYNC fd writes to page cache only.
    // A leader crash before sync() (flushDirtyLogs) may lose the tail.
    // TODO(Milestone C): acks=All waits for ISR HWM before acking.
    // NOTE: the sparse-index entry above is written BEFORE the data.  If the write below fails the
    // entry points at bytes never written — harmless: the log is then failed (no reads/appends) and
    // restart recovery rebuilds the index from the valid data.
    off_t writePos = static_cast<off_t>(logSize_);
    if (storage_ && storage_->writeFaultForTest != 0) [[unlikely]]
        failWrite_(storage_->writeFaultForTest, "LogSegment::appendNoSync pwrite");
    ssize_t written = ::pwrite(fd_, data, len, writePos);
    if (written < 0 || static_cast<std::size_t>(written) != len) [[unlikely]]
        failWrite_(written < 0 ? errno : ENOSPC, "LogSegment::appendNoSync pwrite");   // short write = out of space

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
    if (storage_ && storage_->writeFaultForTest != 0) [[unlikely]]
        failWrite_(storage_->writeFaultForTest, "LogSegment::appendNoSyncRestamp pwritev");
    ssize_t written = ::pwritev(fd_, iov, iovCnt, writePos);
    if (written < 0 || static_cast<std::size_t>(written) != len) [[unlikely]]
        failWrite_(written < 0 ? errno : ENOSPC, "LogSegment::appendNoSyncRestamp pwritev");

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
        // Use pread() via fd_ to avoid fstream mixed read/write mode overhead.
        // On macOS, fstream::seekg() after a write flushes the write buffer
        // through a slow path (~600µs per call observed); pread() is O(1) and
        // bypasses the fstream buffer entirely.  fd_ is O_RDWR | O_APPEND so
        // pread() is legal (pread ignores the file position for the read).
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
            if (total <= 0 || pos + static_cast<size_t>(total) > buf.size()) break;
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
        if (total <= 0 || pos + static_cast<std::size_t>(total) > size) break;
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
        if (total <= 0 || end + static_cast<std::size_t>(total) > size) break;
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
    if (log_.is_open()) log_.flush();   // the durable fstream path: nothing buffered past the cut
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
        if (total <= 0 || pos + static_cast<std::size_t>(total) > size) break;
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
    if (!atCut && next < truncateOffset && next != nextOffset_) [[unlikely]] return EIO;
    if (::ftruncate(fd_, static_cast<off_t>(pos)) != 0) return errno != 0 ? errno : EIO;
    const int64_t lastIdx = index_->truncateToPosition(static_cast<int64_t>(pos));
    logSize_ = static_cast<int64_t>(pos);
    nextOffset_ = next;
    bytesSinceIndex_ = lastIdx < 0 ? 0 : logSize_ - lastIdx;
    if (syncedBytes_ > logSize_) syncedBytes_ = logSize_;
    if (durableNext_ > nextOffset_) durableNext_ = nextOffset_;
    if (durableNext_ < baseOffset_) durableNext_ = baseOffset_;
    keptMaxTs = maxTs;
    return 0;
}

int64_t LogSegment::recover(bool syncIndex) {
    log_.seekg(0, std::ios::end);
    int64_t fileSize = log_.tellg();
    std::vector<uint8_t> buf(static_cast<size_t>(fileSize));
    log_.seekg(0, std::ios::beg);
    if (fileSize > 0) log_.read(reinterpret_cast<char*>(buf.data()), fileSize);

    // B3: rebuild into .index.tmp, then atomically rename over the canonical .index
    // so there is never a window where the on-disk index is stale/orphaned.
    // (The old code wrote to ".index2" and left the real ".index" permanently stale.)
    std::string indexBase = logPath_.substr(0, logPath_.size()-4);
    std::string indexPath = indexBase + ".index";
    std::string indexTmp  = indexBase + ".index.tmp";
    // Remove any leftover .tmp from a previous crash
    std::filesystem::remove(indexTmp);
    index_ = std::make_unique<OffsetIndex>(indexTmp, baseOffset_, indexMaxBytes_);

    size_t pos = 0; int64_t validBytes = 0; int64_t next = baseOffset_;
    // C2: seed bytesSinceIndex_ identically to append() so the index is reproducible.
    bytesSinceIndex_ = 0;
    using rune::protocol::RecordBatchReader;
    while (pos + rune::protocol::batch::kRecordBatchOverhead <= buf.size()) {
        RecordBatchReader r(buf.data() + pos, buf.size() - pos);
        int32_t total = r.totalLength();
        if (total <= 0 || pos + static_cast<size_t>(total) > buf.size() || !r.ensureValid()) break;
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
    std::filesystem::resize_file(logPath_, static_cast<uintmax_t>(validBytes));
    log_.close();
    log_.open(logPath_, std::ios::in | std::ios::out | std::ios::binary);
    logSize_ = validBytes; nextOffset_ = next;
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
        const int e = errno;
        if (storage_) {
            // Task 10 fix 3: fail-stop, not an exception escaping a worker loop
            // (truncateTo on a role change runs on the owning worker).  The log
            // is permanently failed; its owner's loop routes it to the halt.  The
            // rebuilt in-memory index (on the .tmp file) stays valid meanwhile.
            if (storage_->failedErrno == 0) storage_->failedErrno = e != 0 ? e : EIO;
            return nextOffset_;
        }
        throw std::system_error(e, std::system_category(),
                                "LogSegment::recover rename " + indexTmp + " -> " + indexPath);
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
    index_ = std::make_unique<OffsetIndex>(indexPath, baseOffset_, indexMaxBytes_);
    index_->rebuildEntries();

    return nextOffset_;
}
} // namespace rune::storage
