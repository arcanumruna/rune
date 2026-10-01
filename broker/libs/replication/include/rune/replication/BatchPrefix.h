#pragma once
// BatchPrefix.h — the ONE whole-batch walk shared by push and fetch (GR-1).
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/Types.h>

#include <cstddef>
#include <cstdint>

namespace rune::replication {

struct BatchPrefix {
    std::size_t bytes     = 0;    ///< bytes of the whole-batch prefix (0 = nothing fits)
    Offset      endOffset = -1;   ///< last included batch's lastOffset + 1 (-1 if none)
};

/// Largest prefix of [data, data+len) made of WHOLE batches, every batch's
/// baseOffset < ceiling.  Kafka KIP-74: the FIRST whole batch is always included
/// even when it alone exceeds `cap` (otherwise a stored batch larger than a
/// follower's fetch maxBytes is never replicated and the follower stalls at that
/// offset forever — F18); every LATER batch is included only while the total stays
/// <= cap.  A torn tail is ignored.  Pure, allocation-free.  (That the oversized
/// batch fits one replication frame is guaranteed at startup by
/// NodeConfig::validateProduceBatchLimit.)
[[nodiscard]] inline BatchPrefix wholeBatchPrefix(const uint8_t* data, std::size_t len,
                                                  std::size_t cap, Offset ceiling) noexcept
{
    BatchPrefix out;
    const std::size_t kOverhead =
        static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
    std::size_t pos = 0;
    while (data != nullptr && pos + kOverhead <= len) {
        rune::protocol::RecordBatchReader r(data + pos, len - pos);
        const int32_t total = r.totalLength();
        if (total <= 0) break;
        if (r.baseOffset() >= ceiling) break;
        const std::size_t next = pos + static_cast<std::size_t>(total);
        if (next > len) break;                         // torn: never served
        if (pos != 0 && next > cap) break;             // KIP-74: first batch exempt
        out.bytes     = next;
        out.endOffset = r.baseOffset() + r.lastOffsetDelta() + 1;
        pos = next;
    }
    return out;
}

} // namespace rune::replication
