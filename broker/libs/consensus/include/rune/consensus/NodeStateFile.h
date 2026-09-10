#pragma once
// NodeStateFile.h — faithful C++ port of io.aeron.cluster.NodeStateFile
// Durable candidateTermId persistence via mmap with release/acquire fences.
// See: aeron-cluster/src/main/java/io/aeron/cluster/NodeStateFile.java

#include <rune/consensus/Types.h>
#include <atomic>
#include <cstdint>
#include <string>

namespace rune::consensus {

/**
 * Durable storage for a single cluster node's candidateTermId.
 *
 * Backed by a memory-mapped file. The candidateTermId slot is accessed via
 * std::atomic_ref<int64_t> to provide release/acquire fences that match
 * Java's UnsafeBuffer::putLongVolatile / getLongVolatile semantics.
 *
 * File layout (8-byte aligned, fixed offsets):
 *   [0..7]   int64_t  candidateTermId   — volatile slot (release/acquire)
 *   [8..15]  int64_t  timestamp         — epoch-ms of last update (plain)
 *   [16..23] int64_t  logPosition       — log position of last update (plain)
 *   [24..]   padding to kFileSize
 *
 * NULL_VALUE sentinel for candidateTermId = -1 (matching Aeron's Aeron.NULL_VALUE).
 */
class NodeStateFile {
public:
    /// Aeron NULL_VALUE sentinel (matches Java Aeron.NULL_VALUE = -1).
    static constexpr int64_t NULL_VALUE = -1;

    /**
     * Open or create the state file at @p path.
     *
     * Fresh file: initialises candidateTermId = NULL_VALUE (-1), timestamp = 0,
     * logPosition = 0, and calls msync if fileSyncLevel > 0.
     *
     * Existing file: maps it and reads current values.
     *
     * @param path          absolute path to the node-state file.
     * @param fileSyncLevel 0 = no msync; >0 = msync(MS_SYNC) on each write.
     */
    explicit NodeStateFile(std::string path, int fileSyncLevel = 0);
    ~NodeStateFile();

    NodeStateFile(const NodeStateFile&) = delete;
    NodeStateFile& operator=(const NodeStateFile&) = delete;

    /**
     * Read the candidateTermId with ACQUIRE ordering.
     * Mirrors Java: buffer.getLongVolatile(candidateTermIdOffset) [line 364].
     */
    Term candidateTermId() const;

    /**
     * Read the timestamp of the last candidateTermId update (plain read).
     */
    int64_t candidateTermTimestamp() const;

    /**
     * Read the log position of the last candidateTermId update (plain read).
     */
    int64_t candidateTermLogPosition() const;

    /**
     * Propose a new maximum candidateTermId.
     *
     * Exact port of NodeStateFile.java:274-285 and updateCandidateTermId 258-264:
     * - If candidateTermId > current: write logPosition first, then timestamp,
     *   then store candidateTermId LAST with RELEASE ordering (so a concurrent
     *   acquire-reader never sees a new id with stale metadata).
     *   Then msync if fileSyncLevel > 0.
     *   Returns the new candidateTermId.
     * - If candidateTermId <= current: no-op, returns current.
     *
     * @param candidateTermId  proposed new term id
     * @param logPosition      log position at the time of the proposal
     * @param timestampMs      epoch-ms timestamp of the proposal
     * @return max(candidateTermId, current)
     */
    Term proposeMaxCandidateTermId(Term candidateTermId, int64_t logPosition, int64_t timestampMs);

private:
    // Fixed file size: three int64_t fields = 24 bytes, padded to 64 bytes.
    static constexpr std::size_t kFileSize = 64;

    // Byte offsets within the mapped region.
    // candidateTermId is FIRST so it is naturally 8-byte aligned at offset 0.
    static constexpr std::size_t kCandidateTermIdOffset = 0;
    static constexpr std::size_t kTimestampOffset       = 8;
    static constexpr std::size_t kLogPositionOffset     = 16;

    std::string path_;
    int         fileSyncLevel_;
    void*       mapped_    = nullptr;
    int         fd_        = -1;

    // Returns a pointer to an int64_t slot at the given byte offset.
    int64_t* slotAt(std::size_t offset) noexcept {
        return reinterpret_cast<int64_t*>(static_cast<uint8_t*>(mapped_) + offset);
    }
    const int64_t* slotAt(std::size_t offset) const noexcept {
        return reinterpret_cast<const int64_t*>(static_cast<const uint8_t*>(mapped_) + offset);
    }

    // Write all three fields with the correct store order:
    //   logPosition first, timestamp second, candidateTermId LAST (release).
    // Mirrors NodeStateFile.java:258-264 (updateCandidateTermId).
    void updateCandidateTermId(int64_t candidateTermId, int64_t logPosition, int64_t timestampMs);

    void syncFile() const;
    bool fileExists() const;
};

} // namespace rune::consensus
