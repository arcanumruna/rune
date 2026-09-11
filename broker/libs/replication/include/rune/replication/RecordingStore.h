#pragma once
// ---------------------------------------------------------------------------
// RecordingStore.h — Abstract interface for durable recording storage.
//
// Namespace: rune::replication
// C++23. No locks. No busy-spin. Single-writer.
//
// ## Overview (spec §6.5, plan P3-4)
//
// RecordingStore is the storage abstraction behind PartitionRecorder.
// Concrete implementations:
//   - FileRecordingStore  — append-only flat file (works without Aeron Archive)
//   - ArchiveRecordingStore (future, gated on BUILD_AERON_ARCHIVE_TESTS)
//
// ## Batch framing
//
// Each recorded batch is written as:
//   [uint64_t length (8 bytes)] [uint64_t baseOffset (8 bytes)] [batch bytes]
// giving deterministic framing for recovery and partial-tail detection.
// ---------------------------------------------------------------------------

#include <cstdint>
#include <functional>
#include <span>
#include <vector>

namespace rune::replication {

/// Callback invoked by RecordingStore::scan() for each recorded batch.
/// Arguments: baseOffset, batch bytes.
/// Return false to stop iteration early.
using BatchSink = std::function<bool(int64_t baseOffset, std::span<const uint8_t> bytes)>;

// ---------------------------------------------------------------------------
// RecordingStore — abstract interface
// ---------------------------------------------------------------------------
class RecordingStore {
public:
    virtual ~RecordingStore() = default;

    /// Append a committed batch to the store.
    ///
    /// \param baseOffset  The first offset in this batch.
    /// \param bytes       The raw batch bytes.
    virtual void append(int64_t baseOffset, std::span<const uint8_t> bytes) = 0;

    /// Returns the highest recorded position (exclusive end of last batch).
    /// i.e. the offset of the first byte NOT yet recorded.
    /// Initially 0 (nothing recorded).
    [[nodiscard]] virtual int64_t recordingPosition() const noexcept = 0;

    /// Scan recorded batches in offset order, invoking sink for each.
    ///
    /// \param fromOffset  Only deliver batches with baseOffset >= fromOffset.
    /// \param toOffset    Only deliver batches with baseOffset < toOffset
    ///                    (pass INT64_MAX for "all").
    /// \param sink        Callback for each matching batch.
    virtual void scan(int64_t fromOffset, int64_t toOffset,
                      const BatchSink& sink) const = 0;

    /// Total number of batches stored.
    [[nodiscard]] virtual int64_t batchCount() const noexcept = 0;
};

} // namespace rune::replication
