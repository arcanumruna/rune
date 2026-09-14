#include <rune/storage/LogSegment.h>
#include <rune/protocol/RecordBatch.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>
#include <stdexcept>
#include <iomanip>
#include <sstream>
#include <filesystem>
#include <system_error>
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

LogSegment::LogSegment(std::string dir, int64_t baseOffset, int32_t indexIntervalBytes)
    : baseOffset_(baseOffset), indexIntervalBytes_(indexIntervalBytes), nextOffset_(baseOffset) {
    logPath_ = fname(dir, baseOffset, ".log");
    log_.open(logPath_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!log_.is_open()) { std::ofstream(logPath_, std::ios::binary).close();
        log_.open(logPath_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app); }
    // Open a POSIX fd for fsync and direct reads (pread).
    // O_RDWR so we can use pread() from it in read() without the fstream
    // mixed-mode slowdown on macOS.  Writes still go through fstream for
    // simplicity; fd_ is used for fdatasync and pread only.
    fd_ = ::open(logPath_.c_str(), O_RDWR | O_APPEND | O_CREAT, 0644);
    // 8 MiB index map: over-provisioned (~64x) for a 64 MiB segment at 4 KiB interval.
    // Acceptable trade-off: simplifies growth logic. Each segment uses at most 128 KiB
    // of actual index data; the rest is zero-padded and evicted by the OS.
    index_ = std::make_unique<OffsetIndex>(fname(dir, baseOffset, ".index"), baseOffset, 8 * 1024 * 1024);
}

LogSegment::~LogSegment() {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
}

void LogSegment::sync() {
    // 1. Flush libc buffer → OS page cache.
    log_.flush();
    // 2. fsync: flush page cache → storage device (data + size metadata needed to read).
    if (fd_ >= 0) ::fsync(fd_);
    // 3. msync: ensure mmap'd index pages are also written to device.
    index_->sync();
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
    log_.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(len));
    logSize_ += static_cast<int64_t>(len);
    bytesSinceIndex_ += static_cast<int64_t>(len);
    nextOffset_ = lastOffset + 1;
    return nextOffset_;
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
    off_t writePos = static_cast<off_t>(logSize_);
    ssize_t written = ::pwrite(fd_, data, len, writePos);
    if (written < 0 || static_cast<std::size_t>(written) != len) {
        throw std::runtime_error("rune: LogSegment::appendNoSync pwrite failed");
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

int64_t LogSegment::recover() {
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
    index_ = std::make_unique<OffsetIndex>(indexTmp, baseOffset_, 8*1024*1024);

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

    // B3: msync the rebuilt index then atomically rename .index.tmp → .index,
    // then fsync the directory so the renamed entry survives power loss.
    index_->sync();
    if (::rename(indexTmp.c_str(), indexPath.c_str()) != 0) {
        throw std::system_error(errno, std::system_category(),
                                "LogSegment::recover rename " + indexTmp + " -> " + indexPath);
    }
    // fsync the directory for durability of the rename
    {
        std::string dir = logPath_.substr(0, logPath_.rfind('/'));
        int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) { ::fsync(dfd); ::close(dfd); }
    }
    // Reopen index_ from the canonical path and recount entries from mmap data.
    index_ = std::make_unique<OffsetIndex>(indexPath, baseOffset_, 8*1024*1024);
    index_->rebuildEntries();

    return nextOffset_;
}
} // namespace rune::storage
