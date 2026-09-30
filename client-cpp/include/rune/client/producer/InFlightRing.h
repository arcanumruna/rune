// include/rune/client/producer/InFlightRing.h
//
// Batch correlation ids + the fixed per-partition ring of in-flight batch slots
// (spec §4.3, §4.4).  Slots and their frame buffers are reused: after warm-up no
// batch allocates.  A ProduceResponse is matched to its slot by direct index.
#pragma once

#include <cstdint>
#include <vector>

namespace rune::client::producer {

/// Layout of a positive int64 batch correlation id:
///   bit 62       tag — a producer batch id (the Client counter starts at 1 and
///                session correlation bases are < 2^50; neither reaches 2^62)
///   bits 50..61  producer ordinal inside its ProducerHub (<= 4095)
///   bits 32..49  partition slot inside the producer      (<= 262143)
///   bits  0..31  per-partition batch sequence; slot = seq & mask, and an exact
///                corrId comparison rejects stale ids after the slot is reused
namespace batchcorr {
inline constexpr int64_t  kTag          = int64_t{1} << 62;
inline constexpr int      kOrdinalShift = 50;
inline constexpr int      kSlotShift    = 32;
inline constexpr uint32_t kMaxOrdinal   = (1u << 12) - 1;
inline constexpr uint32_t kMaxSlot      = (1u << 18) - 1;

[[nodiscard]] constexpr int64_t make(uint32_t ordinal, uint32_t slot, uint32_t seq) noexcept {
    return kTag | (static_cast<int64_t>(ordinal & kMaxOrdinal) << kOrdinalShift) |
           (static_cast<int64_t>(slot & kMaxSlot) << kSlotShift) | static_cast<int64_t>(seq);
}
[[nodiscard]] constexpr bool isBatch(int64_t c) noexcept { return c > 0 && (c & kTag) != 0; }
[[nodiscard]] constexpr uint32_t ordinal(int64_t c) noexcept {
    return static_cast<uint32_t>(c >> kOrdinalShift) & kMaxOrdinal;
}
[[nodiscard]] constexpr uint32_t slot(int64_t c) noexcept {
    return static_cast<uint32_t>(c >> kSlotShift) & kMaxSlot;
}
[[nodiscard]] constexpr uint32_t seq(int64_t c) noexcept { return static_cast<uint32_t>(c); }
} // namespace batchcorr

struct InFlightBatch {
    int64_t  corrId            = 0;  ///< 0 = free slot
    int64_t  firstSendNs       = 0;
    int64_t  attemptDeadlineNs = 0;
    uint64_t ackStart          = 0;  ///< this batch's slice of the partition ack ring
    int32_t  ackCount          = 0;
    int32_t  recordCount       = 0;
    int32_t  baseSequence      = 0;  ///< idempotent sequence of the batch's first record
    int32_t  ingressStreamId   = 0;
    int32_t  attempts          = 0;
    int32_t  redirectCount     = 0;
    int16_t  producerEpoch     = 0;  ///< epoch the frame was stamped with (unresolved-sequence bump)
    std::vector<uint8_t> wireFrame;  ///< encoded Produce frame; capacity reused
};

class InFlightRing {
public:
    explicit InFlightRing(uint32_t minCapacity = 1) {
        uint32_t cap = 1;
        while (cap < minCapacity) cap <<= 1;
        slots_.resize(cap);
        mask_ = cap - 1;
    }

    [[nodiscard]] uint32_t capacity() const noexcept { return mask_ + 1; }
    [[nodiscard]] uint32_t live() const noexcept { return live_; }
    [[nodiscard]] bool     empty() const noexcept { return live_ == 0; }
    /// True when a claim would overwrite the oldest live slot — holes left by
    /// out-of-order acks still occupy the window until the oldest batch acks.
    [[nodiscard]] bool full() const noexcept {
        return head_ - tail_ >= static_cast<uint64_t>(mask_) + 1;
    }
    /// Sequence the next claim() will use (encode the frame before offering it).
    [[nodiscard]] uint32_t nextSeq() const noexcept { return static_cast<uint32_t>(head_); }

    /// Claims the next slot (caller checked !full()) and stamps its corrId.
    InFlightBatch& claim(uint32_t ordinal, uint32_t partitionSlot) noexcept {
        InFlightBatch& b = slots_[head_ & mask_];
        b.corrId = batchcorr::make(ordinal, partitionSlot, static_cast<uint32_t>(head_));
        ++head_;
        ++live_;
        return b;
    }

    /// The live batch with exactly this corrId, else nullptr (duplicate / stale).
    [[nodiscard]] InFlightBatch* find(int64_t corrId) noexcept {
        InFlightBatch& b = slots_[batchcorr::seq(corrId) & mask_];
        return (corrId != 0 && b.corrId == corrId) ? &b : nullptr;
    }

    /// Frees a live batch (idempotent) and advances the tail past freed slots.
    void release(InFlightBatch& b) noexcept {
        if (b.corrId == 0) return;
        b.corrId = 0;
        --live_;
        while (tail_ != head_ && slots_[tail_ & mask_].corrId == 0) ++tail_;
    }

    [[nodiscard]] InFlightBatch* oldestLive() noexcept {
        return tail_ == head_ ? nullptr : &slots_[tail_ & mask_];
    }

    /// Visits live batches oldest-first.  f may release() the visited batch.
    template <class F>
    void forEachLive(F&& f) {
        const uint64_t end = head_;
        for (uint64_t s = tail_; s != end; ++s) {
            InFlightBatch& b = slots_[s & mask_];
            if (b.corrId != 0) f(b);
        }
    }

private:
    std::vector<InFlightBatch> slots_;
    uint32_t mask_ = 0;
    uint64_t head_ = 0;
    uint64_t tail_ = 0;
    uint32_t live_ = 0;
};

} // namespace rune::client::producer
