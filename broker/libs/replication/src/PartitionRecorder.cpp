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
    // S3 fix: use a proper BYTE budget (64 MiB) rather than the offset count
    // (endOffset - recordFrom), which is much smaller than the actual byte
    // length of multi-record or large-payload batches and caused silent
    // under-recording when batch sizes exceeded the offset count.
    // If the committed range spans more bytes than the 64 MiB cap we loop,
    // advancing recordFrom past each chunk we've successfully processed.
    static constexpr int32_t kMaxReadBytes = 64 * 1024 * 1024; // 64 MiB

    int64_t scanFrom = recordFrom;
    while (scanFrom < endOffset) {
        auto rawBytes = log_->read(scanFrom, kMaxReadBytes);
        if (rawBytes.empty()) {
            break;
        }

        // Walk individual RecordBatch frames within the flat read buffer.
        std::size_t pos = 0;
        bool advanced = false;
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

            if (batchBase >= endOffset) {
                // Past the committed range — stop reading.
                goto done_reading; // NOLINT(cppcoreguidelines-avoid-goto)
            }

            // Only record batches that start within [recordFrom, endOffset).
            if (batchBase >= recordFrom && batchBase < endOffset) {
                store_->append(batchBase,
                               std::span<const uint8_t>(rawBytes.data() + pos,
                                                         static_cast<std::size_t>(batchLen)));
                // Advance scanFrom past this batch for the next read iteration.
                rune::protocol::RecordBatchReader r2(rawBytes.data() + pos,
                                                      rawBytes.size() - pos);
                scanFrom = r2.baseOffset() + r2.lastOffsetDelta() + 1;
                advanced = true;
            }

            pos += static_cast<std::size_t>(batchLen);
        }

        if (!advanced) {
            // No progress in this chunk — stop to avoid an infinite loop.
            break;
        }
    }
    done_reading:;
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
