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
///   bits 24..31  ATTEMPT tag (EOS follow-ups I-2): bumped on every re-send of
///                the batch's stored frame, so an answer names the attempt it
///                answers (Kafka: a new correlation id per request).  An error
///                answer of a superseded attempt is ignored; an OK of any
///                attempt is a success (stored is stored)
///   bits  0..23  per-partition batch sequence; slot = seq & mask, and an exact
///                comparison of the attempt-free id rejects stale ids after the
///                slot is reused.  The id repeats only after 2^24 batches of the
///                same producer and partition: a stale answer can alias a live
///                batch only if it arrives that late — at the ~320k batches/s
///                per-partition ceiling (32 in flight / ~100 us RTT) that is
///                >= ~52 s (N-d), far past any delivery timeout in practice.
///                (Such an alias would be a false success: an OK of any attempt
///                completes the batch.)  The 8-bit attempt tag wraps after 256
///                offers of one batch (>= 256 x produceTimeoutMs of retries).
namespace batchcorr {
inline constexpr int64_t  kTag          = int64_t{1} << 62;
inline constexpr int      kOrdinalShift = 50;
inline constexpr int      kSlotShift    = 32;
inline constexpr uint32_t kMaxOrdinal   = (1u << 12) - 1;
inline constexpr uint32_t kMaxSlot      = (1u << 18) - 1;
inline constexpr int      kAttemptShift = 24;
inline constexpr uint32_t kSeqMask      = (1u << kAttemptShift) - 1;
inline constexpr int64_t  kAttemptMask  = int64_t{0xFF} << kAttemptShift;
// F8c C7 fix round 2 (F2): an ordinal reused by a new producer CARRIES ITS
// SEQUENCES FORWARD — the new incarnation's rings start past the highest
// sequence the previous one ever used on any partition (ProducerHub::remove
// records it).  A late ProduceResponse of a previous incarnation carries a
// sequence below that base and never matches a batch of the new one (exact
// corrId compare) — the only residual is the inherent 32-bit wrap (2^32
// batches on one slot while a response is still outstanding), the same bound
// one producer already relies on against itself.  Zero hot-path cost.

[[nodiscard]] constexpr int64_t make(uint32_t ordinal, uint32_t slot, uint32_t seq,
                                     uint8_t attempt = 0) noexcept {
    return kTag | (static_cast<int64_t>(ordinal & kMaxOrdinal) << kOrdinalShift) |
           (static_cast<int64_t>(slot & kMaxSlot) << kSlotShift) |
           (static_cast<int64_t>(attempt) << kAttemptShift) | static_cast<int64_t>(seq & kSeqMask);
}
/// The id without its attempt tag (what a live slot stores).
[[nodiscard]] constexpr int64_t base(int64_t c) noexcept { return c & ~kAttemptMask; }
[[nodiscard]] constexpr uint8_t attempt(int64_t c) noexcept {
    return static_cast<uint8_t>((c & kAttemptMask) >> kAttemptShift);
}
[[nodiscard]] constexpr int64_t withAttempt(int64_t c, uint8_t a) noexcept {
    return base(c) | (static_cast<int64_t>(a) << kAttemptShift);
}
[[nodiscard]] constexpr bool isBatch(int64_t c) noexcept { return c > 0 && (c & kTag) != 0; }
[[nodiscard]] constexpr uint32_t ordinal(int64_t c) noexcept {
    return static_cast<uint32_t>(c >> kOrdinalShift) & kMaxOrdinal;
}
[[nodiscard]] constexpr uint32_t slot(int64_t c) noexcept {
    return static_cast<uint32_t>(c >> kSlotShift) & kMaxSlot;
}
[[nodiscard]] constexpr uint32_t seq(int64_t c) noexcept { return static_cast<uint32_t>(c) & kSeqMask; }
} // namespace batchcorr

struct InFlightBatch {
    int64_t  corrId            = 0;  ///< 0 = free slot; attempt-free (batchcorr::base)
    uint8_t  attemptTag        = 0;  ///< the attempt of the frame on the wire (batchcorr::attempt)
    int64_t  producerId        = -1; ///< identity the frame was (last) stamped with
    uint64_t lastSendTick      = 0;  ///< ProducerImpl::sendTick_ at the latest offer (ordering, not time)
    int64_t  firstSendNs       = 0;
    int64_t  lastSendNs        = 0;  ///< duty clock of the latest offer of wireFrame
    int64_t  attemptDeadlineNs = 0;
    uint64_t ackStart          = 0;  ///< this batch's slice of the partition ack ring
    int32_t  ackCount          = 0;
    int32_t  recordCount       = 0;
    int32_t  baseSequence      = 0;  ///< idempotent sequence of the batch's first record
    int32_t  ingressStreamId   = 0;
    int32_t  attempts          = 0;
    int32_t  redirectCount     = 0;
    int16_t  producerEpoch     = 0;  ///< epoch the frame was stamped with (unresolved-sequence bump)
    bool     tier2             = false;  ///< F8c C6: a Tier-2 batch (ONE ack: op Tier2Batch; 1 credit unit)
    /// EOS-OOS: the broker answered OutOfOrderSequence while an OLDER batch of
    /// the same epoch was still live (an earlier frame was lost / is unresolved).
    /// Not failed (Kafka parity): re-sent, unchanged, once every older batch has
    /// resolved (PartitionState::resendParked).
    bool     oosParked         = false;
    /// Task 16: queued for an in-order re-offer on the partition's new route
    /// (PartitionState::routeResend); the attempt timer leaves it to that pass.
    bool     routeResendPending = false;
    /// Re-review 1 NEW-4(b): the lane (PartitionState::laneGen) of the latest
    /// offer, the attempt tag of the first offer on it, and the attempts not yet
    /// answered on it / on EARLIER lanes.  Attempts on one lane reach the broker
    /// in order; an earlier lane's attempt may still land after a later answer.
    uint32_t laneGen       = 0;
    uint8_t  laneFirstTag  = 0;
    uint16_t laneOut       = 0;
    uint16_t staleLaneOut  = 0;
    std::vector<uint8_t> wireFrame;  ///< encoded Produce frame; capacity reused
};

class InFlightRing {
public:
    explicit InFlightRing(uint32_t minCapacity = 1) {
        uint32_t cap = 1;
        while (cap < minCapacity) cap <<= 1;
        // The slot index is the low bits of the 24-bit sequence.
        if (cap > batchcorr::kSeqMask + 1u) cap = batchcorr::kSeqMask + 1u;
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
    /// F8c C7 fix round 2 (F2): start the sequence at `base` (the ordinal's
    /// carried-forward range).  Only before the first claim.
    void rebase(uint64_t base) noexcept {
        if (!claimed_) head_ = tail_ = base;
    }
    /// The sequence after the last one this ring ever claimed (cold).
    [[nodiscard]] uint64_t endSeq() const noexcept { return head_; }
    /// Sequence the next claim() will use (encode the frame before offering it).
    [[nodiscard]] uint32_t nextSeq() const noexcept { return static_cast<uint32_t>(head_); }

    /// Claims the next slot (caller checked !full()) and stamps its corrId.
    InFlightBatch& claim(uint32_t ordinal, uint32_t partitionSlot) noexcept {
        InFlightBatch& b = slots_[head_ & mask_];
        b.corrId     = batchcorr::make(ordinal, partitionSlot, static_cast<uint32_t>(head_));
        b.attemptTag = 0;
        ++head_;
        claimed_ = true;
        ++live_;
        return b;
    }

    /// The live batch this corrId (any attempt) names, else nullptr (duplicate / stale).
    [[nodiscard]] InFlightBatch* find(int64_t corrId) noexcept {
        InFlightBatch& b = slots_[batchcorr::seq(corrId) & mask_];
        return (corrId != 0 && b.corrId == batchcorr::base(corrId)) ? &b : nullptr;
    }

    /// Frees a live batch (idempotent) and advances the tail past freed slots.
    void release(InFlightBatch& b) noexcept {
        if (b.corrId == 0) return;
        b.corrId = 0;
        b.routeResendPending = false;  // Task 16: never carried into the slot's next batch
        --live_;
        while (tail_ != head_ && slots_[tail_ & mask_].corrId == 0) ++tail_;
    }

    /// Visits the live batches OLDER than `b` (a live batch of this ring),
    /// oldest-first; stops early when f returns true.  Returns whether it did.
    template <class F>
    bool anyOlderLive(const InFlightBatch& b, F&& f) {
        for (uint64_t s = tail_; s != head_; ++s) {
            InFlightBatch& x = slots_[s & mask_];
            if (&x == &b) return false;
            if (x.corrId != 0 && f(x)) return true;
        }
        return false;
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
    bool     claimed_ = false;  // F2: rebase only before the first claim
};

} // namespace rune::client::producer
