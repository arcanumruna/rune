// ---------------------------------------------------------------------------
// PartitionRecorder.cpp — Records committed partition Log ranges to a
//                         RecordingStore.
// ---------------------------------------------------------------------------

#include <rune/replication/PartitionRecorder.h>
#include <rune/protocol/RecordBatch.h>

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

    // Read all committed bytes from the Log in [recordFrom, endOffset).
    // Log::read returns a flat concatenation of RecordBatch frames.
    // We must walk them one-by-one and append each individually to the
    // RecordingStore so that:
    //   (a) Each batch gets the correct baseOffset in the store, and
    //   (b) recordingPosition() advances by the batch's true lastOffsetDelta
    //       (not just +1) for multi-record batches.
    //
    // This fixes BUG 1: previously only the first batch was recorded when
    // the committed range spanned more than one batch.
    int32_t maxBytes = static_cast<int32_t>(
        std::min<int64_t>(endOffset - recordFrom, 64LL * 1024 * 1024));
    if (maxBytes <= 0) {
        return;
    }

    auto rawBytes = log_->read(recordFrom, maxBytes);
    if (rawBytes.empty()) {
        return;
    }

    // Walk individual RecordBatch frames within the flat read buffer.
    std::size_t pos = 0;
    while (pos < rawBytes.size()) {
        if (rawBytes.size() - pos <
                static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
            break;  // truncated — stop
        }

        rune::protocol::RecordBatchReader reader(rawBytes.data() + pos,
                                                  rawBytes.size() - pos);
        int32_t batchLen = reader.totalLength();
        if (batchLen <= 0 ||
            pos + static_cast<std::size_t>(batchLen) > rawBytes.size()) {
            break;  // incomplete frame — stop
        }

        int64_t batchBase = reader.baseOffset();

        // Only record batches that start within [recordFrom, endOffset).
        if (batchBase >= recordFrom && batchBase < endOffset) {
            store_->append(batchBase,
                           std::span<const uint8_t>(rawBytes.data() + pos,
                                                     static_cast<std::size_t>(batchLen)));
        }

        pos += static_cast<std::size_t>(batchLen);
    }
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
