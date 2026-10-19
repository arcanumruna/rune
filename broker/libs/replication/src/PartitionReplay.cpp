// ---------------------------------------------------------------------------
// PartitionReplay.cpp — Replays recorded batches from a RecordingStore.
// ---------------------------------------------------------------------------

#include <rune/replication/PartitionReplay.h>
#include <rune/protocol/RecordBatch.h>

#include <cassert>
#include <climits>

namespace rune::replication {

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

PartitionReplay::PartitionReplay(const RecordingStore* store)
    : store_(store)
{
    assert(store_ != nullptr && "store must not be null");
}

// ---------------------------------------------------------------------------
// replay
// ---------------------------------------------------------------------------

int64_t PartitionReplay::replay(int64_t fromOffset, int64_t toOffset,
                                 const ReplaySink& sink) const
{
    assert(sink && "sink must not be null");
    if (fromOffset >= toOffset) {
        return 0;
    }

    int64_t count = 0;
    store_->scan(fromOffset, toOffset, [&](int64_t baseOffset,
                                            std::span<const uint8_t> bytes) -> bool {
        bool cont = sink(baseOffset, bytes);
        ++count;
        return cont;
    });

    return count;
}

// ---------------------------------------------------------------------------
// replayAll
// ---------------------------------------------------------------------------

int64_t PartitionReplay::replayAll(const ReplaySink& sink) const
{
    return replay(0, INT64_MAX, sink);
}

// ---------------------------------------------------------------------------
// replayIntoLog
// ---------------------------------------------------------------------------

int64_t PartitionReplay::replayIntoLog(rune::storage::Log& log,
                                        int64_t fromOffset,
                                        int64_t toOffset) const
{
    int64_t count = 0;
    replay(fromOffset, toOffset, [&](int64_t baseOffset,
                                      std::span<const uint8_t> bytes) -> bool {
        // Decode lastOffsetDelta from the RecordBatch header so that
        // multi-record batches advance the log's LEO by the correct amount.
        // For a batch with N records: lastOffset = baseOffset + lastOffsetDelta.
        // For single-record batches lastOffsetDelta == 0, so lastOffset == baseOffset.
        //
        // Passing the wrong lastOffset (e.g. just baseOffset for multi-record batches)
        // would cause the Log to set logEndOffset = baseOffset + 1 instead of
        // baseOffset + lastOffsetDelta + 1, producing gaps on replay.
        int64_t lastOffset = baseOffset;  // default for single-record or undecodable
        if (bytes.size() >=
                static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
            rune::protocol::RecordBatchReader reader(bytes.data(), bytes.size());
            lastOffset = baseOffset + reader.lastOffsetDelta();
        }

        std::vector<uint8_t> buf(bytes.begin(), bytes.end());
        log.append(buf, lastOffset);
        ++count;
        return true;
    });
    return count;
}

} // namespace rune::replication
