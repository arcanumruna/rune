#pragma once
// ---------------------------------------------------------------------------
// BatchHeaderCursor.h - the part of a stored batch's header the CRC does NOT cover (#839).
//
// The batch CRC starts at the attributes field, so baseOffset (bytes 0-7) and magic (16) can
// rot and the batch still verifies.  A scan of one segment therefore also requires, per batch:
//   - magic == kMagicValue,
//   - baseOffset >= the running expected offset (the segment base, then the previous batch's last offset + 1; compaction
//     leaves gaps above it, never below),
//   - the batch's last offset (base + lastOffsetDelta) is < the segment's EXCLUSIVE upper bound and lastOffsetDelta >= 0.
//     The bound is min(segmentBase + 2^31, the next segment's base): OffsetIndex stores offset - segmentBase as an int32,
//     and a segment never reaches into its successor.  baseOffset rotting UPWARD (e.g. one flipped high bit) would
//     otherwise verify and make a reader skip everything / push the LEO far forward.
// A batch that fails any of them ends the valid prefix exactly like a CRC failure.  Cold path (recovery, cleaner): no
// allocation, no virtual.
// ---------------------------------------------------------------------------

#include <rune/protocol/RecordBatch.h>

#include <cstdint>
#include <optional>

namespace rune::storage {

struct BatchHeaderCursor {
    int64_t next;                                              ///< the offset the next batch must start at or above
    int64_t upper_;                                            ///< exclusive upper bound of every offset in the segment

    /// The exclusive offset bound of the segment based at `segBase`: the int32 index range, cut to the successor's base
    /// when the segment is not the last one.  Overflow-safe.
    [[nodiscard]] static int64_t upperFor(int64_t segBase, std::optional<int64_t> nextSegBase = std::nullopt) noexcept {
        const int64_t formatUpper = segBase <= INT64_MAX - (int64_t{INT32_MAX} + 1) ? segBase + int64_t{INT32_MAX} + 1
                                                                                     : INT64_MAX;
        return nextSegBase && *nextSegBase < formatUpper ? *nextSegBase : formatUpper;
    }

    BatchHeaderCursor(int64_t segmentBase, int64_t upperExclusive) noexcept
        : next(segmentBase), upper_(upperExclusive) {}

    /// True (and advances) when the un-CRC-covered header fields of `r` are consistent; false ends the valid prefix.
    [[nodiscard]] bool accept(const rune::protocol::RecordBatchReader& r) noexcept {
        if (r.magic() != rune::protocol::batch::kMagicValue) return false;
        if (r.baseOffset() < next) return false;
        // base is untrusted disk data (may be huge): compare as base >= upper || delta >= upper - base, never base + delta.
        if (r.baseOffset() >= upper_ || r.lastOffsetDelta() < 0 || r.lastOffsetDelta() >= upper_ - r.baseOffset()) return false;
        next = r.baseOffset() + r.lastOffsetDelta() + 1;
        return true;
    }
};

}  // namespace rune::storage
