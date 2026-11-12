#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace rune::node {

/// Bookkeeping for a produce that failed the worker's WRITE check while the session lifecycle ring still held ops.
///
/// A client is told "logged in" / "topic created" once the grant op is ACCEPTED on every worker's lifecycle ring, so a
/// produce can reach a worker before the worker drained that op; such a frame is held (it stays at the head of its
/// client's image and is redelivered).  The frame may be answered once the worker has drained everything that was on
/// the ring when it FIRST saw the frame: ops that arrive later cannot be the grant the client was told about.  Each
/// held frame therefore remembers the ring's producer position at first sight (its target).
///
/// Invariant: a mark never causes an early answer (Answer needs consumerPos >= producerPos at first sight), and a
/// frame without a mark (table full) falls back to "hold while the ring is non-empty", the old, always-safe rule.
///
/// Worker thread only; no allocation, no virtual; cold path (denied produces only).
class GrantHoldMarks {
public:
    static constexpr std::size_t kCap = 64;
    enum class Verdict : uint8_t { Hold, Answer };

    /// A produce of `sessionId` with `corr` failed the WRITE check.  producerPos / consumerPos are the lifecycle
    /// ring's positions NOW.  Answer = everything that was on the ring when this frame was first seen is applied.
    [[nodiscard]] Verdict onDenied(int64_t sessionId, int64_t corr, int64_t producerPos, int64_t consumerPos) noexcept {
        for (uint32_t i = 0; i < n_; ++i) {
            if (marks_[i].sessionId != sessionId) continue;
            if (marks_[i].corr == corr) {
                if (consumerPos >= marks_[i].target) {
                    erase_(i);
                    return Verdict::Answer;
                }
                return Verdict::Hold;
            }
            erase_(i);   // another frame of this session is at the head now: it starts its own mark
            break;
        }
        if (producerPos <= consumerPos) return Verdict::Answer;   // nothing pending, no mark
        if (n_ < kCap) {
            marks_[n_++] = Mark{sessionId, corr, producerPos};
            return Verdict::Hold;
        }
        for (uint32_t i = 0; i < n_; ++i) {
            if (marks_[i].target <= consumerPos) {   // its frame is due; if it comes back it is held one more pass
                marks_[i] = Mark{sessionId, corr, producerPos};
                return Verdict::Hold;
            }
        }
        ++unmarkedHolds_;
        return Verdict::Hold;
    }

    [[nodiscard]] std::size_t size() const noexcept { return n_; }
    [[nodiscard]] uint64_t unmarkedHolds() const noexcept { return unmarkedHolds_; }   // holds taken with the table full

private:
    struct Mark {
        int64_t sessionId;
        int64_t corr;
        int64_t target;
    };
    void erase_(uint32_t i) noexcept {
        marks_[i] = marks_[n_ - 1];
        --n_;
    }
    std::array<Mark, kCap> marks_{};
    uint32_t n_ = 0;
    uint64_t unmarkedHolds_ = 0;
};

}  // namespace rune::node
