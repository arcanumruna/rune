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
//     answers ack-only; after kFetchResendNs stuck it serves data (lost image).
//   * F27 PER-FOLLOWER PUSH CURSOR: pushedThrough advances ONLY when the push to
//     THIS follower was accepted by the transport (never on a refused send), and
//     ackedLeo is the follower's own last-reported position in this leadership;
//     the next push to it starts at pushCursor() = max(pushedThrough, ackedLeo).
//     A refused push is therefore re-sent on the next pass from that follower's
//     own cursor (time-independent) without re-sending to followers that got it.
//   * F27 OWED ANSWER: a fetch answer the transport refused is owed — the leader
//     re-sends it on its next pass (no wait for the follower's resend timer) and
//     the refused serve is never remembered for conflation.
// Single-writer: the thread that owns the replica.  Reset on every role change.
#include <rune/replication/ReplicationMessages.h>   // OffsetsForLeaderEpochRespMsg (owed DR answer)
#include <rune/replication/Types.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace rune::replication {

// 20 ms re-serve window.  F27: NOT a correctness mechanism for a refused send —
// those are retried on the next pass.  Aeron is reliable and ordered once a frame
// is ACCEPTED, so this only backstops a frame lost with its image (peer restart /
// image loss), where the follower's fetch position stops advancing.
inline constexpr int64_t kFetchResendNs = 20'000'000;

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
    // F27: the follower's last fetchOffset in THIS leadership (-1 = not heard yet).
    Offset  ackedLeo          = -1;
    // F27: a fetch answer the transport refused, re-sent on the leader's next pass.
    // fetchOffset < 0 = nothing owed.  Superseded by the follower's next fetch.
    struct OwedAnswer {
        Offset  fetchOffset = -1;
        int32_t maxBytes    = 0;
        bool    data        = false;   ///< Data serve (records) vs HW-only answer
        int16_t errorCode   = 0;       ///< HW-only answer's error code (kErrNone = 0)
        // F7 memo: the whole-batch walk of a Data answer is rebuilt only when the
        // leader's LEO moved since it was built (a dead follower's retry is then
        // a readView + send, never a re-walk).
        Offset         builtLeo        = -1;
        std::size_t    builtBytes      = 0;
        Offset         builtEnd        = -1;
        // R2: a compaction swap can remap/rewrite the bytes at the SAME LEO — the
        // memo is valid only for the same view and the same log install identity.
        const uint8_t* builtView       = nullptr;
        std::size_t    builtLen        = 0;
        uint64_t       builtInstallGen = 0;
    };
    OwedAnswer owed{};
    // F6: exclusive end of Data this follower's FETCH answers delivered (accepted)
    // in this leadership — the push cursor skips it (no duplicate push).  Never
    // feeds the AckOnly gate (catch-up semantics unchanged).
    Offset dataServedThrough = -1;
    // F27 F3: an OffsetsForLeaderEpoch answer the transport refused, re-sent on
    // the next pass; superseded by the follower's next probe.
    struct OwedEpochAnswer {
        bool                         valid = false;
        OffsetsForLeaderEpochRespMsg msg{};
    };
    OwedEpochAnswer owedEpoch{};
    // F27 R1: the follower's connection generation last seen on its fetch (0 =
    // never seen).  Survives resetLeadership(): it is a property of the link.
    uint32_t connGen = 0;

    /// F27 R1: the link to this follower went down and came back: anything the
    /// transport accepted while it was down may be lost — forget every claim of
    /// "sent" above its reported position so it is re-sent from there.
    void rewindForReconnect(Offset fetchOffset) noexcept {
        if (pushedThrough > fetchOffset) pushedThrough = fetchOffset;
        dataServedThrough = -1;
        catchUpTarget     = -1;
        owed              = OwedAnswer{};
        forgetServe();
    }

    /// Where the next push to this follower starts (F27 per-follower cursor).
    [[nodiscard]] Offset pushCursor() const noexcept {
        Offset c = pushedThrough > ackedLeo ? pushedThrough : ackedLeo;
        return dataServedThrough > c ? dataServedThrough : c;
    }
    /// F27: the answer to the current fetch was REFUSED by the transport — forget
    /// it as served (an identical retry must never be conflated against it).
    void forgetServe() noexcept {
        servedFetchOffset = -1; servedLeo = -1; servedTimeNs = 0;
    }

    void resetLeadership() noexcept {
        servedFetchOffset = -1; servedLeo = -1; servedTimeNs = 0;
        askOffset = -1; askSinceNs = 0; pushedThrough = 0; catchUpTarget = -1;
        ackedLeo = -1; owed = OwedAnswer{}; dataServedThrough = -1; owedEpoch = OwedEpochAnswer{};
        // connGen is NOT reset: it belongs to the link, not the leadership — a
        // bounce across a re-election must still rewind (re-review 2 N1).
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
    [[nodiscard]] const FollowerSlot* find(NodeId id) const noexcept {
        for (std::size_t i = 0; i < n_; ++i) if (slots_[i].id == id) return &slots_[i];
        return nullptr;
    }
    void resetLeadership() noexcept {
        for (std::size_t i = 0; i < n_; ++i) slots_[i].resetLeadership();
    }
    [[nodiscard]] std::size_t size() const noexcept { return n_; }
    [[nodiscard]] FollowerSlot& at(std::size_t i) noexcept { return slots_[i]; }

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
