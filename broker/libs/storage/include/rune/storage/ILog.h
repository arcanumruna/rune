#pragma once
// ---------------------------------------------------------------------------
// ILog.h — pure-virtual seam for the partition log interface.
//
// Implemented by:
//   rune::storage::Log        — on-disk append-only log (segments + mmap index)
//   rune::storage::InMemoryLog — bounded in-memory ring (zero-disk, zero-alloc
//                                steady-state, drop-oldest eviction)
//
// The subset exposed here covers ONLY the methods called by:
//   • PartitionReplica  (append/appendNoSync/read/logEndOffset/truncateTo)
//   • ProducerStateManager::rebuildFromLog
//       (read/logEndOffset/logStartOffset/segmentBaseOffsets)
//
// All other Log methods (cleaner API, pre-provisioning, SPSC rings, etc.) are
// concrete-Log-only concerns and are NOT part of this interface.
//
// Namespace: rune::storage   C++23
// ---------------------------------------------------------------------------
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace rune::storage {

class ILog {
public:
    virtual ~ILog() = default;

    // -----------------------------------------------------------------------
    // Append
    // -----------------------------------------------------------------------

    /// Append [data, data+len) as a batch with the given lastOffset.
    /// Durable by default (calls sync after writing).
    virtual int64_t append(const uint8_t* data, std::size_t len, int64_t lastOffset) = 0;

    /// Vector overload (delegates to span overload in concrete impls).
    virtual int64_t append(const std::vector<uint8_t>& batch, int64_t lastOffset) = 0;

    /// No-sync append — writes without calling sync().
    /// Gateway group-commit path: batch appends, then sync() once.
    virtual int64_t appendNoSync(const uint8_t* data, std::size_t len, int64_t lastOffset) = 0;

    // -----------------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------------

    /// Read records starting at startOffset, up to maxBytes.
    /// Returns empty if startOffset < logStartOffset() (out-of-range).
    virtual std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes) = 0;

    // -----------------------------------------------------------------------
    // Offset accessors
    // -----------------------------------------------------------------------

    /// Exclusive end offset (next offset to be written).
    [[nodiscard]] virtual int64_t logEndOffset() const noexcept = 0;

    /// Inclusive start offset — the lowest offset still readable.
    /// Returns 0 for an empty log.
    [[nodiscard]] virtual int64_t logStartOffset() const noexcept = 0;

    // -----------------------------------------------------------------------
    // Mutation
    // -----------------------------------------------------------------------

    /// Truncate all records with baseOffset >= truncateOffset.
    /// Resets logEndOffset to truncateOffset.
    virtual void truncateTo(int64_t truncateOffset) = 0;

    // -----------------------------------------------------------------------
    // Durability / recovery
    // -----------------------------------------------------------------------

    /// Flush + fsync.  No-op for InMemoryLog.
    virtual void sync() = 0;

    /// Recover from disk / initialise.  No-op for InMemoryLog.
    virtual int64_t recover() = 0;

    // -----------------------------------------------------------------------
    // Metadata
    // -----------------------------------------------------------------------

    /// Directory containing this log's segment files, or a synthetic name for
    /// in-memory implementations.
    [[nodiscard]] virtual const std::string& dir() const noexcept = 0;

    /// Base offsets of all segments (ascending).  For InMemoryLog returns a
    /// single-element vector {logStartOffset()} so that PSM rebuildFromLog's
    /// CP-3/CP-4 segment-gap logic works correctly (InMemoryLog has no gaps).
    [[nodiscard]] virtual std::vector<int64_t> segmentBaseOffsets() const = 0;
};

} // namespace rune::storage
