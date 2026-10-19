#pragma once
// ---------------------------------------------------------------------------
// ProducerIdScan.h — scanMaxProducerId: walk a partition log up to HW and
// return the highest producerId seen across all batch headers.
//
// Namespace: rune::storage
// C++23.  Header-only template (like ProducerStateManager::rebuildFromLog).
//
// ## Design
//
// This is a pure scan with NO ProducerStateManager involvement — it only looks
// at batch header producerId fields.  The result feeds
// rune::transaction::ProducerIdAllocator::reseedAtLeast() so the allocator
// never re-issues a PID that already exists in the durable log.
//
// Template parameter LogT: concrete log type — rune::storage::Log or
// rune::storage::InMemoryLog.  NO ILog / virtual dispatch (de-virtualized
// seam, Milestone D Phase 2).
//
// HW semantics: mirror ProducerStateManager::rebuildFromLog — scan only
// batches whose baseOffset < highWatermark (committed HW, NOT LEO) so
// uncommitted batches that may be truncated by KIP-101 divergence resolution
// do NOT advance the allocator.
// ---------------------------------------------------------------------------

#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <cstdint>
#include <vector>

namespace rune::storage {

// ---------------------------------------------------------------------------
// scanMaxProducerId<LogT>
//
// Walks all RecordBatches in [logStartOffset, highWatermark) and returns the
// maximum producerId field observed (−1 if no idempotent batch found).
//
// @param log           Concrete log instance (Log or InMemoryLog). Non-const
//                      because LogT::read() is non-const in both implementations.
// @param highWatermark Exclusive upper bound (committed HW).
// @return              Max producerId seen, or -1 if no idempotent batch exists.
// ---------------------------------------------------------------------------
template<class LogT>
[[nodiscard]] int64_t scanMaxProducerId(LogT& log, int64_t highWatermark) noexcept
{
    if (highWatermark <= 0) return -1;

    static constexpr int32_t kChunkBytes = 64 * 1024 * 1024; // 64 MiB per read

    // Start at the first live segment base (mirrors rebuildFromLog CP-3).
    int64_t fetchOffset = 0;
    {
        const auto segBases = log.segmentBaseOffsets();
        if (!segBases.empty() && segBases.front() > 0) {
            fetchOffset = segBases.front();
        }
    }

    int64_t maxPid = -1;

    while (fetchOffset < highWatermark) {
        std::vector<uint8_t> data = log.read(fetchOffset, kChunkBytes);
        if (data.empty()) {
            // CP-4: gap in a compacted segment — skip to the next segment.
            const auto segBases = log.segmentBaseOffsets();
            auto it = std::upper_bound(segBases.begin(), segBases.end(), fetchOffset);
            if (it == segBases.end()) break;
            fetchOffset = *it;
            continue;
        }

        std::size_t pos          = 0;
        int64_t     lastBatchEnd = fetchOffset;

        while (pos < data.size()) {
            const uint8_t*    batchStart = data.data() + pos;
            const std::size_t remaining  = data.size() - pos;

            if (remaining < static_cast<std::size_t>(
                    rune::protocol::batch::kRecordBatchOverhead))
                break;

            rune::protocol::RecordBatchReader reader(batchStart, remaining);

            const int32_t tl = reader.totalLength();
            if (tl <= 0 || static_cast<std::size_t>(tl) > remaining) break;

            if (!reader.ensureValid()) break;

            const int64_t batchBase = reader.baseOffset();
            const int32_t lastDelta = reader.lastOffsetDelta();
            const int64_t batchEnd  = batchBase + lastDelta + 1;

            // Stop at HW (TX-2b: don't read uncommitted tail).
            if (batchBase >= highWatermark) break;

            const int64_t pid = reader.producerId();
            // Only idempotent producers have a non-negative producerId.
            if (pid >= 0 && pid > maxPid) {
                maxPid = pid;
            }

            if (batchEnd > lastBatchEnd) lastBatchEnd = batchEnd;
            pos += static_cast<std::size_t>(tl);
        }

        if (lastBatchEnd <= fetchOffset) break; // no progress
        fetchOffset = lastBatchEnd;
    }

    return maxPid;
}

} // namespace rune::storage
