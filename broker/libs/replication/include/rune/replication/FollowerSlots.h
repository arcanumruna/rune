#pragma once
// FollowerSlots.h — leader-side per-follower fetch bookkeeping (spec §3.5).
//
// One fixed-size flat array indexed by the follower's slot in the partition's
// replica set (no map, no allocation).  Holds, per follower:
//   * FETCH CONFLATION (formerly the servedFetch_ unordered_map): the last
//     (fetchOffset, leader LEO, time) served — an IDENTICAL request inside
//     kFetchResendNs gets no response.
//   * NO-DUP-FETCH progress tracker: the offset the follower is asking for and
//     since when, plus the exclusive end of data pushed to THIS follower in THIS
//     leadership.  While the follower advances inside the pushed range the leader
//     answers ack-only; after kFetchResendNs stuck it serves data (lost push).
// Single-writer: the thread that owns the replica.  Reset on every role change.
#include <rune/replication/Types.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace rune::replication {

inline constexpr int64_t kFetchResendNs = 20'000'000;  // 20 ms lost-push re-serve window

struct FollowerSlot {
    NodeId  id                = -1;
    Offset  servedFetchOffset = -1;
    Offset  servedLeo         = -1;
    int64_t servedTimeNs      = 0;
    Offset  askOffset         = -1;
    int64_t askSinceNs        = 0;
    Offset  pushedThrough     = 0;
    // CATCH-UP (review I1): once a gapped/stuck follower is served Data by a
    // FETCH, keep serving Data until its fetch offset reaches this target (the
    // pushedThrough at the last catch-up serve).  Data served by a fetch is not a
    // push, so it must never re-arm the ack-only gate.  -1 = not catching up.
    Offset  catchUpTarget     = -1;

    void resetLeadership() noexcept {
        servedFetchOffset = -1; servedLeo = -1; servedTimeNs = 0;
        askOffset = -1; askSinceNs = 0; pushedThrough = 0; catchUpTarget = -1;
    }
};

enum class FetchServe : uint8_t { Conflate, AckOnly, Data };

class FollowerSlots {
public:
    static constexpr std::size_t kMaxReplicas = 16;

    /// Assign slots in replica-set order.  false (nothing assigned) when the set
    /// exceeds kMaxReplicas — the caller refuses construction.
    [[nodiscard]] bool init(const std::vector<NodeId>& replicas) noexcept {
        if (replicas.size() > kMaxReplicas) return false;
        n_ = replicas.size();
        for (std::size_t i = 0; i < n_; ++i) {
            slots_[i] = FollowerSlot{};
            slots_[i].id = replicas[i];
        }
        return true;
    }
    [[nodiscard]] FollowerSlot* find(NodeId id) noexcept {
        for (std::size_t i = 0; i < n_; ++i) if (slots_[i].id == id) return &slots_[i];
        return nullptr;
    }
    void resetLeadership() noexcept {
        for (std::size_t i = 0; i < n_; ++i) slots_[i].resetLeadership();
    }
    [[nodiscard]] std::size_t size() const noexcept { return n_; }

private:
    std::array<FollowerSlot, kMaxReplicas> slots_{};
    std::size_t n_ = 0;
};

/// Pure decision for one FetchRequest; updates the slot's conflation + progress state.
[[nodiscard]] inline FetchServe decideFetchServe(FollowerSlot& s, Offset fetchOffset,
                                                 int32_t maxBytes, Offset curLeo,
                                                 bool pushOnAppend, int64_t nowNs,
                                                 int64_t resendNs) noexcept {
    if (s.servedTimeNs != 0 && s.servedFetchOffset == fetchOffset &&
        s.servedLeo == curLeo && (nowNs - s.servedTimeNs) < resendNs) {
        return FetchServe::Conflate;
    }
    s.servedFetchOffset = fetchOffset;
    s.servedLeo         = curLeo;
    s.servedTimeNs      = nowNs;
    if (maxBytes <= 0) return FetchServe::AckOnly;              // pure fast-ack: HW only
    // A fetch BELOW the previous ask means the follower lost data (restart/truncate):
    // it is not inside the push stream, so it enters catch-up at once.
    const bool wentBack = s.askOffset >= 0 && fetchOffset < s.askOffset;
    if (fetchOffset != s.askOffset) { s.askOffset = fetchOffset; s.askSinceNs = nowNs; }
    if (s.catchUpTarget >= 0) {
        if (fetchOffset < s.catchUpTarget) {                    // still catching up:
            s.catchUpTarget = s.pushedThrough;                  // pushes it dropped meanwhile
            return FetchServe::Data;                            // extend the target
        }
        s.catchUpTarget = -1;                                   // reached the push frontier
    }
    if (pushOnAppend && fetchOffset < s.pushedThrough) {
        if (!wentBack && (nowNs - s.askSinceNs) < resendNs) {
            return FetchServe::AckOnly;                         // covered by a push, still advancing
        }
        s.catchUpTarget = s.pushedThrough;                      // stuck (lost push) or went back
    }
    return FetchServe::Data;                                    // never pushed, stuck, or catching up
}

} // namespace rune::replication
