// ---------------------------------------------------------------------------
// PartitionRecorder.cpp — Records committed partition Log ranges to a
//                         RecordingStore.
// ---------------------------------------------------------------------------

#include <rune/replication/PartitionRecorder.h>

#include <cassert>
#include <stdexcept>

namespace rune::replication {

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

PartitionRecorder::PartitionRecorder(PartitionId         partitionId,
                                     rune::storage::Log* log,
                                     RecordingStore*     store)
    : partitionId_(partitionId)
    , log_(log)
    , store_(store)
{
    assert(log_   != nullptr && "log must not be null");
    assert(store_ != nullptr && "store must not be null");
}

// ---------------------------------------------------------------------------
// startRecording
// ---------------------------------------------------------------------------

void PartitionRecorder::startRecording(int64_t fromOffset)
{
    if (recording_) {
        // Idempotent — already recording.
        return;
    }
    fromOffset_ = fromOffset;
    recording_  = true;
}

// ---------------------------------------------------------------------------
// onCommitted
// ---------------------------------------------------------------------------

void PartitionRecorder::onCommitted(int64_t startOffset, int64_t endOffset)
{
    if (!recording_) {
        return;
    }

    // Clamp startOffset to our fromOffset_.
    int64_t recordFrom = (startOffset < fromOffset_) ? fromOffset_ : startOffset;
    if (recordFrom >= endOffset) {
        return;  // nothing to record in range
    }

    // Read committed batches from the Log in [recordFrom, endOffset).
    // Log::read returns a flat concatenation of batches; we record them as a
    // single append per onCommitted call (batches are already framed inside).
    //
    // We record from recordFrom up to endOffset, capped by what the Log holds.
    int32_t maxBytes = static_cast<int32_t>(
        std::min<int64_t>(endOffset - recordFrom, 64LL * 1024 * 1024));
    if (maxBytes <= 0) {
        return;
    }

    auto bytes = log_->read(recordFrom, maxBytes);
    if (bytes.empty()) {
        return;
    }

    // Append to store.  baseOffset = recordFrom (the start of this recorded chunk).
    store_->append(recordFrom, std::span<const uint8_t>(bytes.data(), bytes.size()));
}

// ---------------------------------------------------------------------------
// getRecordingPosition
// ---------------------------------------------------------------------------

int64_t PartitionRecorder::getRecordingPosition() const noexcept
{
    return store_->recordingPosition();
}

// ---------------------------------------------------------------------------
// stopRecording
// ---------------------------------------------------------------------------

void PartitionRecorder::stopRecording()
{
    recording_ = false;
}

} // namespace rune::replication
