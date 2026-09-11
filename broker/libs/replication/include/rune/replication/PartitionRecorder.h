#pragma once
// ---------------------------------------------------------------------------
// PartitionRecorder.h — Records committed partition Log ranges to a
//                       RecordingStore.
//
// Namespace: rune::replication
// C++23. No locks. No busy-spin. Single-writer.
//
// ## Overview (spec §6.5, plan P3-4)
//
// PartitionRecorder sits between the partition leader and its durable backing
// store.  Its job is simple: as the leader advances the high-watermark and
// commits record batches, PartitionRecorder appends those batches to a
// RecordingStore for persistence.
//
// API mirrors Aeron Archive semantics:
//   startRecording(partition, fromOffset) — begin recording from a given Log offset.
//   onCommitted(startOffset, endOffset)   — notify that [start,end) is committed;
//                                           recorder fetches and stores the bytes.
//   getRecordingPosition()               — highest recorded offset (exclusive).
//   stopRecording()                      — stop recording (idempotent).
//
// ## Single-writer rule
//
// The same thread that drives the partition leader's doWork() must call
// onCommitted().  No internal locks; no concurrent calls.
//
// ## Dependency injection
//
// PartitionRecorder holds a non-owning pointer to a RecordingStore and a
// non-owning pointer to the partition's Log.  The caller owns both.
// ---------------------------------------------------------------------------

#include <rune/replication/RecordingStore.h>
#include <rune/storage/Log.h>
#include <rune/replication/Types.h>

#include <cstdint>
#include <string>

namespace rune::replication {

// ---------------------------------------------------------------------------
// PartitionRecorder
// ---------------------------------------------------------------------------
class PartitionRecorder {
public:
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// \param partitionId   The partition being recorded.
    /// \param log           Non-owning pointer to the partition Log.
    /// \param store         Non-owning pointer to the RecordingStore backing.
    PartitionRecorder(PartitionId         partitionId,
                      rune::storage::Log* log,
                      RecordingStore*     store);

    ~PartitionRecorder() = default;

    // Non-copyable.
    PartitionRecorder(const PartitionRecorder&)            = delete;
    PartitionRecorder& operator=(const PartitionRecorder&) = delete;

    // -----------------------------------------------------------------------
    // Recording lifecycle (mirrors Aeron Archive API)
    // -----------------------------------------------------------------------

    /// Start recording from the given Log offset.
    ///
    /// \param fromOffset  The first Log offset to record.  Batches with
    ///                    baseOffset < fromOffset are skipped.
    ///
    /// May only be called once (or after stopRecording()).
    void startRecording(int64_t fromOffset = 0);

    /// Notify the recorder that the committed range [startOffset, endOffset)
    /// is now available in the Log.
    ///
    /// The recorder reads batches from the Log in that range and appends them
    /// to the RecordingStore.
    ///
    /// \param startOffset  First committed offset (inclusive).
    /// \param endOffset    One past the last committed offset (exclusive).
    ///
    /// No-op if not currently recording.
    void onCommitted(int64_t startOffset, int64_t endOffset);

    /// Returns the position through which recording is complete.
    ///
    /// Mirrors Aeron Archive getRecordingPosition(): the next offset that
    /// will be recorded, i.e. the exclusive end of the last recorded batch.
    [[nodiscard]] int64_t getRecordingPosition() const noexcept;

    /// Stop recording.  Idempotent.
    void stopRecording();

    /// Whether recording is currently active.
    [[nodiscard]] bool isRecording() const noexcept { return recording_; }

    /// The partition being recorded.
    [[nodiscard]] PartitionId partitionId() const noexcept { return partitionId_; }

private:
    PartitionId         partitionId_;
    rune::storage::Log* log_;        ///< non-owning
    RecordingStore*     store_;      ///< non-owning
    bool                recording_  = false;
    int64_t             fromOffset_ = 0;   ///< first offset to record
};

} // namespace rune::replication
