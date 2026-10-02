// examples/bench/SeqTimestampRing.h — per-partition timestamps indexed by the
// producer's idempotent sequence (BatchAck::baseSequence + i), so batches may be
// acknowledged in any order.  Storage is allocated once in the constructor;
// push() / take() never allocate.
#pragma once

#include <cstdint>
#include <vector>

namespace rune::bench {

class SeqTimestampRing {
public:
    explicit SeqTimestampRing(uint32_t minCapacity) {
        uint32_t cap = 1;
        while (cap < minCapacity) cap <<= 1;
        slots_.resize(cap);
        mask_ = cap - 1;
    }
    /// True when the next push would overwrite the oldest not-yet-taken slot.
    [[nodiscard]] bool     full() const noexcept { return next_ - oldest_ >= slots_.size(); }
    [[nodiscard]] uint64_t nextSeq() const noexcept { return next_; }
    /// Sequences in [nextSeq() - outstanding(), nextSeq()) are not yet all taken.
    [[nodiscard]] uint64_t outstanding() const noexcept { return next_ - oldest_; }

    void push(int64_t ts) noexcept {  // caller checked !full()
        slots_[next_ & mask_] = Slot{ts, false};
        ++next_;
    }

    /// seq must be in [nextSeq() - outstanding(), nextSeq()) and not yet taken.
    int64_t take(uint64_t seq) noexcept {
        Slot& s = slots_[seq & mask_];
        s.taken = true;
        while (oldest_ < next_ && slots_[oldest_ & mask_].taken) ++oldest_;
        return s.ts;
    }

private:
    struct Slot {
        int64_t ts    = 0;
        bool    taken = true;
    };
    std::vector<Slot> slots_;
    uint64_t          mask_   = 0;
    uint64_t          next_   = 0;
    uint64_t          oldest_ = 0;
};

} // namespace rune::bench
