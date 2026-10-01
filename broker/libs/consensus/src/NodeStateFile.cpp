#include <rune/consensus/NodeStateFile.h>

// POSIX mmap/msync/ftruncate
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

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
        // candidateTermId — same write order as an update (single owner thread).
        *slotAt(kLogPositionOffset) = 0;
        *slotAt(kTimestampOffset)   = 0;
        *slotAt(kCandidateTermIdOffset) = NULL_VALUE;

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
    // F9a: plain load — the mapping is consensus-thread-owned (see header).
    return static_cast<Term>(*slotAt(kCandidateTermIdOffset));
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
//   buffer.putLongVolatile(candidateTermIdOffset, id); // last (plain here, F9a)
//   syncFile(mappedFile);
void NodeStateFile::updateCandidateTermId(
    int64_t candidateTermId, int64_t logPosition, int64_t timestampMs)
{
    // Aeron's write order: logPosition and timestamp BEFORE the new
    // candidateTermId.
    *slotAt(kLogPositionOffset) = logPosition;
    *slotAt(kTimestampOffset)   = timestampMs;

    // Aeron writes candidateTermId LAST (putLongVolatile).  The mapping has a
    // single owner thread (F9a), so a plain store keeps the order that matters
    // to the owner; persistence is msync's job (syncFile below).
    *slotAt(kCandidateTermIdOffset) = candidateTermId;

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
