// examples/bench/PartitionTimestamps.h — one partition's ack -> scheduled-time
// attribution for the invoker bench (Aeron-free, unit-tested).
//
// The sender push()es each accepted record's scheduled send time in send order.
// An idempotent producer (acks leader/all) stamps the partition's records with
// consecutive sequences in that same order, so a BatchAck (baseSequence,
// recordCount) names its records' slots exactly, whatever order the acks
// arrive in.  acks=None batches are unsequenced (baseSequence -1) and complete
// in send order: they are attributed FIFO.
//
// Anything that cannot be attributed exactly — an epoch change (sequences
// restart at 0), an unsequenced ack from a sequenced producer (a record failed
// before it was sequenced), a sequence that was never sent or is already taken
// — returns false and records nothing: the bench then stops the run rather
// than report a latency it cannot vouch for.
#pragma once

#include "SeqTimestampRing.h"

#include <cstdint>

namespace rune::bench {

class PartitionTimestamps {
public:
    PartitionTimestamps(uint32_t minCapacity, bool sequenced) : ring_(minCapacity), sequenced_(sequenced) {}

    [[nodiscard]] bool     full() const noexcept { return ring_.full(); }
    [[nodiscard]] uint64_t outstanding() const noexcept { return ring_.outstanding(); }
    void                   push(int64_t scheduledNs) noexcept { ring_.push(scheduledNs); }

    /// f(scheduledNs) for each of the ack's records; false (nothing recorded)
    /// when the ack cannot be attributed exactly.
    template <class F>
    [[nodiscard]] bool onAck(int32_t baseSequence, int32_t recordCount, int16_t epoch, F&& f) noexcept {
        if (recordCount <= 0) return recordCount == 0;
        uint64_t first = 0;
        if (sequenced_) {
            if (baseSequence < 0) return false;
            if (!haveEpoch_) {
                epoch_     = epoch;
                haveEpoch_ = true;
            } else if (epoch != epoch_) {
                return false;
            }
            first = static_cast<uint64_t>(baseSequence);
        } else {
            first = fifo_;
        }
        const uint64_t end = first + static_cast<uint64_t>(recordCount);
        for (uint64_t s = first; s < end; ++s) {
            if (!ring_.pending(s)) return false;
        }
        for (uint64_t s = first; s < end; ++s) f(ring_.take(s));
        if (!sequenced_) fifo_ = end;
        return true;
    }

private:
    SeqTimestampRing ring_;
    bool             sequenced_;
    bool             haveEpoch_ = false;
    int16_t          epoch_     = 0;
    uint64_t         fifo_      = 0;  // unsequenced: next record to attribute
};

} // namespace rune::bench
