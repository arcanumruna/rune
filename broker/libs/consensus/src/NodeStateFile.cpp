#include <rune/consensus/NodeStateFile.h>

// POSIX mmap/msync/ftruncate
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <system_error>

namespace rune::consensus {

// ---------------------------------------------------------------------------
// Construction helpers
// ---------------------------------------------------------------------------

bool NodeStateFile::fileExists() const {
    struct stat st{};
    return ::stat(path_.c_str(), &st) == 0;
}

NodeStateFile::NodeStateFile(std::string path, int fileSyncLevel)
    : path_(std::move(path)), fileSyncLevel_(fileSyncLevel)
{
    const bool fresh = !fileExists();

    // Open or create the backing file.
    int flags = O_RDWR | O_CREAT;
    fd_ = ::open(path_.c_str(), flags, 0644);
    if (fd_ < 0) {
        throw std::system_error(errno, std::system_category(),
                                "NodeStateFile: open " + path_);
    }

    // Ensure the file is exactly kFileSize bytes.
    if (::ftruncate(fd_, static_cast<off_t>(kFileSize)) != 0) {
        ::close(fd_);
        throw std::system_error(errno, std::system_category(),
                                "NodeStateFile: ftruncate " + path_);
    }

    // Map the region.
    void* p = ::mmap(nullptr, kFileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (p == MAP_FAILED) {
        ::close(fd_);
        throw std::system_error(errno, std::system_category(),
                                "NodeStateFile: mmap " + path_);
    }
    mapped_ = p;

    if (fresh) {
        // Initialise a fresh file:
        //   candidateTermId = NULL_VALUE (-1), timestamp = 0, logPosition = 0.
        // Write logPosition and timestamp as plain stores first, then
        // candidateTermId with release ordering — same discipline as an update.
        // (On a fresh file there are no concurrent readers, but we apply the
        // same fence discipline for correctness by construction.)
        *slotAt(kLogPositionOffset) = 0;
        *slotAt(kTimestampOffset)   = 0;
        // Release store so any reader that sees NULL_VALUE also sees 0/0.
        std::atomic_ref<int64_t>(*slotAt(kCandidateTermIdOffset))
            .store(NULL_VALUE, std::memory_order_release);

        syncFile();
    }
    // Existing file: values are already present in the mapping.
}

NodeStateFile::~NodeStateFile() {
    if (mapped_) {
        ::munmap(mapped_, kFileSize);
        mapped_ = nullptr;
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

// Mirrors Java NodeStateFile.java:362-365:
//   buffer.getLongVolatile(candidateTermIdOffset)
Term NodeStateFile::candidateTermId() const {
    // atomic_ref requires a non-const reference; cast away const on the pointer
    // (the mapping is PROT_READ|PROT_WRITE and the data itself is not const).
    int64_t& slot = *const_cast<int64_t*>(slotAt(kCandidateTermIdOffset));
    return static_cast<Term>(
        std::atomic_ref<int64_t>(slot).load(std::memory_order_acquire));
}

int64_t NodeStateFile::candidateTermTimestamp() const {
    return *slotAt(kTimestampOffset);
}

int64_t NodeStateFile::candidateTermLogPosition() const {
    return *slotAt(kLogPositionOffset);
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

// Mirrors Java NodeStateFile.java:258-264 (updateCandidateTermId):
//   buffer.putLong(logPositionOffset, logPosition);   // plain
//   buffer.putLong(timestampOffset, timestampMs);      // plain
//   buffer.putLongVolatile(candidateTermIdOffset, id); // release
//   syncFile(mappedFile);
void NodeStateFile::updateCandidateTermId(
    int64_t candidateTermId, int64_t logPosition, int64_t timestampMs)
{
    // Write order matters: logPosition and timestamp BEFORE the new
    // candidateTermId, so a reader that acquires the new id always sees
    // up-to-date metadata.
    *slotAt(kLogPositionOffset) = logPosition;
    *slotAt(kTimestampOffset)   = timestampMs;

    // RELEASE store — the memory_order_release fence ensures the two plain
    // stores above are visible to any thread that subsequently does an
    // acquire-load of candidateTermId.
    // Mirrors Java's putLongVolatile (volatile write = release in JMM).
    std::atomic_ref<int64_t>(*slotAt(kCandidateTermIdOffset))
        .store(candidateTermId, std::memory_order_release);

    syncFile();
}

// Mirrors Java NodeStateFile.java:274-285 (proposeMaxCandidateTermId):
//   if (candidateTermId > existingCandidateTermId)
//       updateCandidateTermId(...)
//       return candidateTermId
//   return existingCandidateTermId
Term NodeStateFile::proposeMaxCandidateTermId(
    Term candidateTermId, int64_t logPosition, int64_t timestampMs)
{
    // ACQUIRE read — mirrors Java's getLongVolatile.
    const Term current = this->candidateTermId();

    if (candidateTermId > current) {
        updateCandidateTermId(
            static_cast<int64_t>(candidateTermId), logPosition, timestampMs);
        return candidateTermId;
    }

    return current;
}

// ---------------------------------------------------------------------------
// Sync
// ---------------------------------------------------------------------------

void NodeStateFile::syncFile() const {
    if (fileSyncLevel_ > 0) {
        // S3 fix: propagate msync errors — EIO on writeback must not be silenced.
        // An msync failure means the data may not reach stable storage.
        if (::msync(mapped_, kFileSize, MS_SYNC) != 0) {
            throw std::system_error(errno, std::system_category(),
                                    "NodeStateFile: msync " + path_);
        }
        // B1 fix: fsync the fd after msync to guarantee the data reaches stable
        // storage before the vote is granted. Mirrors Aeron ConsensusModule which
        // runs with fileSyncLevel>0 for exactly this reason.
        // Reference: phase2-notes §A.4 "Never grant two votes in one term across a crash".
        if (::fsync(fd_) != 0) {
            throw std::system_error(errno, std::system_category(),
                                    "NodeStateFile: fsync " + path_);
        }
    }
}

} // namespace rune::consensus
