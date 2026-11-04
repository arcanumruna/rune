#pragma once
// ---------------------------------------------------------------------------
// BatchHeaderCursor.h - the part of a stored batch's header the CRC does NOT cover (#839).
//
// The batch CRC starts at the attributes field, so baseOffset (bytes 0-7), partitionLeaderEpoch (12-15) and magic (16) can
// rot and the batch still verifies.  A scan of one segment therefore also requires, per batch:
//   - magic == kMagicValue,
//   - baseOffset >= the running expected offset (the segment base, then the previous batch's last offset + 1; compaction
//     leaves gaps above it, never below),
//   - partitionLeaderEpoch >= the previous batch's epoch in the same segment (epochs never go down within a log).
// A batch that fails any of them ends the valid prefix exactly like a CRC failure.  Cold path (recovery, cleaner): no
// allocation, no virtual.
// ---------------------------------------------------------------------------

#include <rune/protocol/RecordBatch.h>

#include <cstdint>
#include <limits>

namespace rune::storage {

struct BatchHeaderCursor {
    int64_t next;                                              ///< the offset the next batch must start at or above
    int32_t prevEpoch = std::numeric_limits<int32_t>::min();   ///< the previous accepted batch's epoch

    explicit BatchHeaderCursor(int64_t segmentBase) noexcept : next(segmentBase) {}

    /// True (and advances) when the un-CRC-covered header fields of `r` are consistent; false ends the valid prefix.
    [[nodiscard]] bool accept(const rune::protocol::RecordBatchReader& r) noexcept {
        if (r.magic() != rune::protocol::batch::kMagicValue) return false;
        if (r.baseOffset() < next) return false;
        const int32_t epoch = r.partitionLeaderEpoch();
        if (epoch < prevEpoch) return false;
        prevEpoch = epoch;
        next = r.baseOffset() + r.lastOffsetDelta() + 1;
        return true;
    }
};

}  // namespace rune::storage
