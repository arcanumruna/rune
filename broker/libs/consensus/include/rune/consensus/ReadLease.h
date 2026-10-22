#pragma once
// ReadLease.h — issue #82 (VLS Part A, broker/docs/superpowers/specs/2026-10-19-view-read-lease-and-sink-plane-design.md):
// the controller-granted, quorum-capped broker READ LEASE that gates every view read.  Pure logic: no clock, no I/O, no
// allocation after construction; the caller passes every time (always the LEASE clock, LeaseClock.h).
//
// Why: a source-partition leader cut off from the controller keeps role_ == Leader until it learns its demotion, so a
// view read through it answered from its frozen table after the new leader acked newer writes.  A broker now serves a
// view row only while it holds a lease, and a lease provably ends before any other broker can lead that partition.
//
// ── Assumptions (A.3) ─────────────────────────────────────────────────────────────────────────────────────────────────
//   C1 bounded drift: every lease clock runs at real rate within [1/(1+rho), 1+rho], rho = 1 % (kLeaseDriftBudget).
//   C2 the lease clock counts suspend (LeaseClock.h).
//   C3 no two clocks are compared: every bound is a local duration.
//   C4 PreVote stickiness: a voter that heard the live controller within H (leaderHeartbeatTimeoutNs) denies every
//      (pre-)vote, so no new controller exists before a majority has gone H without hearing it.  A lease GRANT is a
//      "heard" source (Election::onReadLeaseGrant refreshes leaderHeardNs_ and the follower liveness).
//
// ── Controller: ReadLeaseGrantor ──────────────────────────────────────────────────────────────────────────────────────
//   Every grant gets a sequence number `g` and its send time on the controller clock (a ring per member).  A request
//   echoes the highest grant sequence that member HEARD.  t* = the latest send time such that a majority (the controller
//   included, at `now`) echoed a grant sent at or after it.  By C4 no new controller exists before real t* + H/(1+rho);
//   on the controller clock that is QL = t* + H/(1+rho)^2.  A grant is valid for
//        validForNs = clamp(min(L, QL - now), 0, L),   L = kReadLeaseMaxNs
//   and the controller records leaseExpiry[member] = max(.., now + validForNs): it never fails a broker's partitions over
//   while a lease it granted may still run (Election::eligibleNodeIds).
//
// ── Broker: ReadLeaseHolder ───────────────────────────────────────────────────────────────────────────────────────────
//   Records each request's send time s (lease clock).  A grant for that request gives leaseEnd = s + validFor/(1+rho)^2.
//   Proof that the broker's lease ends (real time) before the controller's bound:  broker real end <= tau_s + V/(1+rho)
//   (its clock is at least 1/(1+rho) fast), tau_s <= tau_grant, and the controller's bound is >= tau_grant + V/(1+rho).
//   Every lease carries the controller's metadata log end `commitPos` at grant time: the broker hands a lease to a worker
//   only once every role op derived from the metadata below commitPos is ON that worker's op ring (ReadLeaseFanout), so a
//   grant issued after a demotion was committed never reaches a worker before the demotion does (FIFO).
#include <rune/consensus/Types.h>

#include <algorithm>
#include <array>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <span>

namespace rune::consensus {

/// L: the longest lease a grant may carry (A.4).  In practice QL caps it near H/(1+rho)^2 minus the echo age (~110-130 ms
/// at the defaults H = 150 ms, status interval 20 ms).
inline constexpr int64_t kReadLeaseMaxNs = 200'000'000;
/// rho = 1 %: (1+rho)^2 = 10201 / 10000.
inline constexpr int64_t kLeaseDriftNum = 10'201;
inline constexpr int64_t kLeaseDriftDen = 10'000;

/// floor(ns / (1+rho)^2), for 0 <= ns (no overflow for any int64).
[[nodiscard]] constexpr int64_t leaseShrinkByDrift(int64_t ns) noexcept {
    if (ns <= 0) return 0;
    return (ns / kLeaseDriftNum) * kLeaseDriftDen + ((ns % kLeaseDriftNum) * kLeaseDriftDen) / kLeaseDriftNum;
}
/// ceil(ns * (1+rho)^2), for 0 <= ns < INT64_MAX / 2.
[[nodiscard]] constexpr int64_t leaseGrowByDrift(int64_t ns) noexcept {
    if (ns <= 0) return 0;
    const int64_t extra = (ns / kLeaseDriftDen) * (kLeaseDriftNum - kLeaseDriftDen) +
                          ((ns % kLeaseDriftDen) * (kLeaseDriftNum - kLeaseDriftDen) + kLeaseDriftDen - 1) / kLeaseDriftDen;
    return ns + extra;
}
/// W (A.5.3): how long a broker that just became a source leader waits (its own lease clock) before it may assume every
/// lease a predecessor held under grants issued before the leader change has ended: Lmax * (1+rho)^2 with Lmax = H (a
/// grant never exceeds QL - now <= H/(1+rho)^2).
[[nodiscard]] constexpr int64_t leaseFenceWaitNs(int64_t heardTimeoutNs) noexcept { return leaseGrowByDrift(heardTimeoutNs); }

// =====================================================================================================================
// ReadLeaseGrantor — the controller's side.  Duty-cycle (control) thread only.
// =====================================================================================================================
class ReadLeaseGrantor {
public:
    static constexpr std::size_t kMaxMembers = 32;   // NodeId < 32 (the election's member masks)
    static constexpr std::size_t kGrantRing  = 16;   // grants remembered per member (~320 ms at the 20 ms cadence)

    struct Grant {
        int64_t grantSeq   = -1;   ///< -1: not granted (unknown member)
        int64_t validForNs = 0;
    };

    /// Construction (cold): the cluster members (self included), this node, H and L.
    void configure(std::span<const NodeId> members, NodeId self, int64_t heardTimeoutNs,
                   int64_t maxLeaseNs = kReadLeaseMaxNs) noexcept {
        n_ = 0;
        for (const NodeId m : members) {
            if (m < 0 || static_cast<std::size_t>(m) >= kMaxMembers || n_ >= kMaxMembers) continue;
            members_[n_++] = Member{m};
        }
        self_          = self;
        heardTimeoutNs_ = heardTimeoutNs;
        maxLeaseNs_    = maxLeaseNs;
    }

    /// A new leadership of this node (any term): the echoes of an earlier leadership say nothing about this one (its
    /// followers' stickiness is for whichever leader they heard), so they are forgotten.  The grant sequence keeps
    /// counting (it is unique for the process lifetime, so an echo of an old grant never matches a new one), and every
    /// leaseExpiry is kept (a grant of an earlier leadership of this node may still run).
    void onLeadership() noexcept {
        for (std::size_t i = 0; i < n_; ++i) {
            members_[i].echoSendNs = kNever;
            members_[i].ring       = {};
        }
    }

    /// A ReadLeaseRequest from `member` (term already checked by the caller) at `nowNs`, echoing the highest grant
    /// sequence it heard.  Records the echo, computes the grant, remembers its send time and the member's lease expiry.
    Grant onRequest(NodeId member, int64_t heardGrantSeq, int64_t nowNs) noexcept {
        Member* m = find_(member);
        if (m == nullptr || member == self_) return {};
        if (heardGrantSeq > 0) {
            const Slot& s = m->ring[static_cast<std::size_t>(heardGrantSeq) % kGrantRing];
            if (s.seq == heardGrantSeq && s.sendNs > m->echoSendNs) m->echoSendNs = s.sendNs;
        }
        Grant g;
        g.validForNs = validForNs(nowNs);
        g.grantSeq   = ++grantSeq_;
        m->ring[static_cast<std::size_t>(g.grantSeq) % kGrantRing] = Slot{g.grantSeq, nowNs};
        if (g.validForNs > 0) m->leaseExpiryNs = std::max(m->leaseExpiryNs, nowNs + g.validForNs);
        return g;
    }

    /// QL (controller clock): t* + H/(1+rho)^2, t* = the latest echo time a majority (this node at `nowNs`) reached.
    [[nodiscard]] int64_t quorumLeaseEndNs(int64_t nowNs) const noexcept {
        if (n_ == 0) return kNever;
        std::array<int64_t, kMaxMembers> t{};
        for (std::size_t i = 0; i < n_; ++i) t[i] = members_[i].id == self_ ? nowNs : members_[i].echoSendNs;
        const std::size_t k = n_ / 2 + 1;   // the quorum threshold (Aeron ClusterMember.quorumThreshold)
        std::nth_element(t.begin(), t.begin() + static_cast<std::ptrdiff_t>(k - 1), t.begin() + static_cast<std::ptrdiff_t>(n_),
                         [](int64_t a, int64_t b) { return a > b; });
        const int64_t tStar = t[k - 1];
        if (tStar == kNever) return kNever;
        return tStar + leaseShrinkByDrift(heardTimeoutNs_);
    }

    /// clamp(min(L, QL - now), 0, L).
    [[nodiscard]] int64_t validForNs(int64_t nowNs) const noexcept {
        const int64_t ql = quorumLeaseEndNs(nowNs);
        if (ql == kNever || ql <= nowNs) return 0;
        return std::min(maxLeaseNs_, ql - nowNs);
    }

    /// The controller's own lease (it serves view reads as a broker too): valid for validForNs(now) from now, recorded
    /// like a grant (eligibility includes self anyway).
    [[nodiscard]] int64_t selfValidForNs(int64_t nowNs) noexcept {
        const int64_t v = validForNs(nowNs);
        if (Member* m = find_(self_); m != nullptr && v > 0) m->leaseExpiryNs = std::max(m->leaseExpiryNs, nowNs + v);
        return v;
    }

    /// A lease this controller granted to `member` may still run at `nowNs` (controller clock): its partitions are never
    /// failed over before it ends (Election::eligibleNodeIds).
    [[nodiscard]] bool leaseMayRun(NodeId member, int64_t nowNs) const noexcept {
        const Member* m = find_(member);
        return m != nullptr && m->leaseExpiryNs > nowNs;
    }
    [[nodiscard]] int64_t leaseExpiryNs(NodeId member) const noexcept {
        const Member* m = find_(member);
        return m != nullptr ? m->leaseExpiryNs : kNever;
    }
    [[nodiscard]] int64_t echoSendNsForTest(NodeId member) const noexcept {
        const Member* m = find_(member);
        return m != nullptr ? m->echoSendNs : kNever;
    }
    [[nodiscard]] int64_t grantSeqForTest() const noexcept { return grantSeq_; }

    static constexpr int64_t kNever = INT64_MIN / 4;

private:
    struct Slot { int64_t seq = 0; int64_t sendNs = kNever; };
    struct Member {
        NodeId id = -1;
        int64_t echoSendNs    = kNever;   ///< the send time of the latest grant this member echoed (this leadership)
        int64_t leaseExpiryNs = kNever;   ///< max(grant send + validFor) over every grant to it
        std::array<Slot, kGrantRing> ring{};
    };
    [[nodiscard]] Member* find_(NodeId id) noexcept {
        for (std::size_t i = 0; i < n_; ++i) if (members_[i].id == id) return &members_[i];
        return nullptr;
    }
    [[nodiscard]] const Member* find_(NodeId id) const noexcept {
        for (std::size_t i = 0; i < n_; ++i) if (members_[i].id == id) return &members_[i];
        return nullptr;
    }
    std::array<Member, kMaxMembers> members_{};
    std::size_t n_ = 0;
    NodeId  self_ = -1;
    int64_t heardTimeoutNs_ = 0;
    int64_t maxLeaseNs_ = kReadLeaseMaxNs;
    int64_t grantSeq_ = 0;
};

// =====================================================================================================================
// ReadLeaseHolder — every broker's side (the controller's own self-grants included).  Duty-cycle (control) thread only.
// =====================================================================================================================
class ReadLeaseHolder {
public:
    static constexpr std::size_t kRequestRing = 64;   // requests remembered (~1.3 s at the 20 ms cadence)
    static constexpr std::size_t kRecent      = 8;    // granted leases kept for delivery (~160 ms of grants)

    struct Request { int64_t reqSeq = 0; int64_t heardGrantSeq = 0; };
    /// One granted lease: valid until endNs (lease clock) once the metadata below commitPos is applied.
    struct Lease { int64_t endNs = kNone; int64_t commitPos = 0; };
    static constexpr int64_t kNone = INT64_MIN / 4;

    /// A request about to be sent at `nowNs` (lease clock): its sequence and the echo.
    Request nextRequest(int64_t nowNs) noexcept {
        const int64_t seq = ++reqSeq_;
        sent_[static_cast<std::size_t>(seq) % kRequestRing] = Sent{seq, nowNs};
        return Request{seq, heardGrantSeq_};
    }

    /// A grant from the leader this node follows (the caller checked the sender, the term and the recipient).  The echo is
    /// recorded even for a zero lease (that is how the controller's QL starts to advance).  false: an unknown request.
    bool onGrant(int64_t reqSeq, int64_t grantSeq, int64_t commitPos, int64_t validForNs) noexcept {
        if (reqSeq <= 0) return false;
        const Sent& s = sent_[static_cast<std::size_t>(reqSeq) % kRequestRing];
        if (s.seq != reqSeq) return false;
        if (grantSeq > heardGrantSeq_) heardGrantSeq_ = grantSeq;
        if (validForNs <= 0) return true;
        push_(Lease{s.sendNs + leaseShrinkByDrift(std::min(validForNs, kReadLeaseMaxNs)), commitPos});
        return true;
    }

    /// The controller's own lease, valid for `validForNs` from `nowNs`.
    void onSelfGrant(int64_t nowNs, int64_t validForNs, int64_t commitPos) noexcept {
        if (validForNs <= 0) return;
        push_(Lease{nowNs + leaseShrinkByDrift(std::min(validForNs, kReadLeaseMaxNs)), commitPos});
    }

    /// A new leader / term to follow: its grant sequence numbers are its own (the echo restarts from none).  Granted
    /// leases are kept: each is bounded on its own.
    void onNewLeader() noexcept { heardGrantSeq_ = 0; }

    /// The longest lease deliverable once this node applied the metadata below `appliedPos` (kNone: none).
    [[nodiscard]] int64_t deliverableEndNs(int64_t appliedPos) const noexcept {
        int64_t best = kNone;
        for (const Lease& l : recent_) if (l.endNs > best && l.commitPos <= appliedPos) best = l.endNs;
        return best;
    }
    [[nodiscard]] int64_t latestEndNs() const noexcept {
        int64_t best = kNone;
        for (const Lease& l : recent_) best = std::max(best, l.endNs);
        return best;
    }
    [[nodiscard]] int64_t heardGrantSeq() const noexcept { return heardGrantSeq_; }
    [[nodiscard]] uint64_t leasesGranted() const noexcept { return granted_; }

private:
    struct Sent { int64_t seq = 0; int64_t sendNs = kNone; };
    void push_(Lease l) noexcept {
        recent_[next_] = l;
        next_ = (next_ + 1) % kRecent;
        ++granted_;
    }
    std::array<Sent, kRequestRing> sent_{};
    std::array<Lease, kRecent>     recent_{};
    std::size_t next_ = 0;
    int64_t  reqSeq_        = 0;
    int64_t  heardGrantSeq_ = 0;
    uint64_t granted_       = 0;
};

} // namespace rune::consensus
