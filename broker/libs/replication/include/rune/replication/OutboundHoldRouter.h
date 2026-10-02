#pragma once
// ---------------------------------------------------------------------------
// OutboundHoldRouter.h — CONTROL-thread routing of the legacy outbound ring to
// per-peer publications, with a per-peer HELD FIFO (F27 fix round 1, F1).
//
// AeronPartitionTransport's workers enqueue replication frames on an MPSC ring;
// the control thread drains it and offers each frame to its peer's publication.
// Once the ring ACCEPTED a frame the worker's replica treats it as sent (its push
// cursor advanced), so the control thread must NEVER drop it:
//
//   * Phase A — flushHeld(): every peer with held frames offers them IN ORDER;
//     the first refusal stops THAT peer only (the others keep flushing).
//   * Phase B — route() per ring frame (controlled read):
//       unicast to P:  P unknown           → routing error, counted, consumed
//                      P's FIFO non-empty  → appended behind it (order kept)
//                      P's FIFO empty      → offered; refused (back-pressure,
//                                            admin action, not connected, pub
//                                            unresolved) → appended to the FIFO
//       broadcast:     the unicast rule per peer — only a refusing peer holds a
//                      copy; every non-empty FIFO is checked for room FIRST so a
//                      broadcast is never applied to only some peers.
//       a full FIFO    → route() returns false = ABORT: the frame stays in the
//                      ring and is re-read next drain.  Nothing is dropped.
//   One slow peer never blocks the others until it has absorbed a full ring of
//   backlog (dead/stuck); only then does the shared ring fill and the workers'
//   own F27 retry (send returns false) take over.
//   The only frames ever discarded are the permanent refusals: an unknown target
//   (routing bug) and an oversized frame (F18: unreachable by construction).
//
// FIFO memory: one fixed byte ring per peer, allocated ONCE at addPeer (cold),
// never reallocated.  Records are [u32 len][frame] 4-byte aligned; a record never
// wraps (the tail end is skipped with a wrap marker).  Control thread only — no
// atomics, shared-nothing.  The Offer policy is a TEMPLATE parameter (no virtual,
// no std::function): int64_t offer(NodeId peer, const uint8_t* buf, size_t len),
// > 0 accepted, kOfferOversized permanent refusal, any other value refused.
// ---------------------------------------------------------------------------
#include <rune/replication/Types.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <vector>

namespace rune::replication {

/// Offer result for a frame that can NEVER be sent (exceeds the publication's
/// maxMessageLength) — discarded and counted, never held.
inline constexpr int64_t kOfferOversized = std::numeric_limits<int64_t>::min();

class HeldFifo {
public:
    /// Cold: allocate the fixed capacity (bytes, rounded down to a multiple of 4).
    void init(std::size_t capacityBytes) {
        cap_ = capacityBytes & ~static_cast<std::size_t>(3);
        buf_.assign(cap_, 0);
        head_ = tail_ = 0;
        count_ = 0;
    }
    [[nodiscard]] bool        empty() const noexcept { return count_ == 0; }
    [[nodiscard]] std::size_t size()  const noexcept { return count_; }

    [[nodiscard]] bool hasRoom(std::size_t len) const noexcept { return need_(len) != 0; }

    bool push(const uint8_t* p, std::size_t len) noexcept {
        if (count_ == 0) head_ = tail_ = 0;           // empty: restart contiguous at 0
        const std::size_t need = need_(len);
        if (need == 0) return false;
        const std::size_t rec   = rec_(len);
        std::size_t       idx   = tail_ % cap_;
        if (cap_ - idx < rec) {                       // skip the tail end: wrap marker
            putU32_(idx, kWrap);
            tail_ += cap_ - idx;
            idx = 0;
        }
        putU32_(idx, static_cast<uint32_t>(len));
        if (len) std::memcpy(buf_.data() + idx + 4, p, len);
        tail_ += rec;
        ++count_;
        return true;
    }

    /// Head frame (FIFO non-empty).
    [[nodiscard]] std::span<const uint8_t> front() noexcept {
        const std::size_t idx = headIndex_();
        return {buf_.data() + idx + 4, getU32_(idx)};
    }
    void pop() noexcept {
        const std::size_t idx = headIndex_();
        head_ += rec_(getU32_(idx));
        if (--count_ == 0) head_ = tail_ = 0;
    }

private:
    static constexpr uint32_t kWrap = 0xFFFFFFFFu;
    static std::size_t rec_(std::size_t len) noexcept { return (4 + len + 3) & ~static_cast<std::size_t>(3); }
    /// Bytes the push consumes (0 = no room).
    [[nodiscard]] std::size_t need_(std::size_t len) const noexcept {
        const std::size_t rec = rec_(len);
        if (cap_ == 0 || rec > cap_) return 0;
        if (count_ == 0) return rec;                  // push() restarts at 0
        const std::size_t idx  = tail_ % cap_;
        const std::size_t need = rec + ((cap_ - idx < rec) ? cap_ - idx : 0);
        return (cap_ - (tail_ - head_) >= need) ? need : 0;
    }
    std::size_t headIndex_() noexcept {
        std::size_t idx = head_ % cap_;
        if (getU32_(idx) == kWrap) { head_ += cap_ - idx; idx = 0; }
        return idx;
    }
    void     putU32_(std::size_t i, uint32_t v) noexcept { std::memcpy(buf_.data() + i, &v, 4); }
    uint32_t getU32_(std::size_t i) const noexcept { uint32_t v; std::memcpy(&v, buf_.data() + i, 4); return v; }

    std::vector<uint8_t> buf_;
    std::size_t          cap_   = 0;
    uint64_t             head_  = 0;   // monotonic byte positions (mod cap_)
    uint64_t             tail_  = 0;
    std::size_t          count_ = 0;
};

struct OutboundHoldStats {
    uint64_t held            = 0;   ///< frames put in a FIFO (refused on first offer / queued behind)
    uint64_t flushed         = 0;   ///< held frames later accepted
    uint64_t unroutable      = 0;   ///< unicast to an unknown peer (routing error)
    uint64_t oversizeDropped = 0;   ///< permanent refusal (F18 — never expected)
    uint64_t aborts          = 0;   ///< ring reads aborted on a full FIFO
};

template <class Offer>
class OutboundHoldRouter {
public:
    static constexpr int32_t kBroadcast = -1;

    using Stats = OutboundHoldStats;

    /// Cold: register a peer with a FIFO of `fifoBytes` (allocated once).
    void addPeer(NodeId id, std::size_t fifoBytes) {
        for (auto& p : peers_) if (p.id == id) return;
        peers_.push_back(Peer{id, {}});
        peers_.back().fifo.init(fifoBytes);
    }

    /// Phase A: flush held frames in order; a refusal stops that peer only.
    /// Returns frames flushed (work).
    int flushHeld(Offer& offer) noexcept {
        int n = 0;
        for (auto& p : peers_) {
            while (!p.fifo.empty()) {
                const auto f = p.fifo.front();
                const int64_t r = offer(p.id, f.data(), f.size());
                if (r > 0)                       { p.fifo.pop(); ++n; ++stats_.flushed; }
                else if (r == kOfferOversized)   { p.fifo.pop(); ++stats_.oversizeDropped; }
                else break;                      // refused: keep it, next peer
            }
        }
        return n;
    }

    /// Phase B: route ONE ring frame.  true = consumed; false = ABORT (leave the
    /// frame in the ring — a FIFO that must hold it is full).
    bool route(Offer& offer, int32_t target, const uint8_t* wire, std::size_t len) noexcept {
        if (target == kBroadcast) {
            for (auto& p : peers_)
                if (!p.fifo.empty() && !p.fifo.hasRoom(len)) { ++stats_.aborts; return false; }
            for (auto& p : peers_) deliver_(offer, p, wire, len);
            return true;
        }
        Peer* p = find_(target);
        if (p == nullptr) { ++stats_.unroutable; return true; }
        if (!p->fifo.empty() && !p->fifo.hasRoom(len)) { ++stats_.aborts; return false; }
        deliver_(offer, *p, wire, len);
        return true;
    }

    [[nodiscard]] std::size_t heldFrames(NodeId id) const noexcept {
        for (const auto& p : peers_) if (p.id == id) return p.fifo.size();
        return 0;
    }
    [[nodiscard]] const Stats& stats() const noexcept { return stats_; }

private:
    struct Peer { NodeId id; HeldFifo fifo; };

    Peer* find_(NodeId id) noexcept {
        for (auto& p : peers_) if (p.id == id) return &p;
        return nullptr;
    }
    // Precondition: an empty FIFO, or one with room for len (checked by route()).
    void deliver_(Offer& offer, Peer& p, const uint8_t* wire, std::size_t len) noexcept {
        if (p.fifo.empty()) {
            const int64_t r = offer(p.id, wire, len);
            if (r > 0) return;
            if (r == kOfferOversized) { ++stats_.oversizeDropped; return; }
        }
        (void)p.fifo.push(wire, len);   // behind earlier held frames, or refused now
        ++stats_.held;
    }

    std::vector<Peer> peers_;
    Stats             stats_{};
};

} // namespace rune::replication
