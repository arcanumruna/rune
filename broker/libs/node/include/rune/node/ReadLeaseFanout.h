#pragma once
// ReadLeaseFanout.h — issue #82 (VLS Part A, A.5.1): the control thread hands this broker's view read lease to every
// worker, FIFO behind the role ops it covers.
//
// A grant carries `commitPos`, the controller's metadata log end when it was sent.  A demotion committed before the
// grant sits below commitPos, so the lease reaches a worker only once
//   (1) this broker applied the metadata below commitPos to its committed map (MetadataService::appliedEnd, captured
//       before the bridge's pass), and the bridge derived that map's role ops in this same pass, and
//   (2) none of those role ops still waits for the worker's op ring (ReplicationMetadataBridge::roleOpsPendingFor) -
//       every one of them is on the ring, ahead of the lease.
// The worker executes its op ring in order, so it applies the demotion before it can extend its lease with a grant
// issued after the demotion committed: a lease never overtakes a demotion.  Grants issued before it are bounded on their
// own (they end before any other broker can lead, ReadLease.h).
//
// Per worker the state is the lease end it has been handed (monotone; a newer deliverable lease is always later).  The
// holder keeps the last few grants, so continuous metadata churn (a newest grant always slightly ahead of the applied
// position) still delivers the newest COVERED one.  A full ring is retried next pass (nothing lost; only the newest
// matters).  Control thread only; no allocation after configure().
#include <rune/consensus/ReadLease.h>

#include <cstdint>
#include <vector>

namespace rune::node {

class ReadLeaseFanout {
public:
    /// Construction (cold): `workers` workers, all without a lease.
    void configure(int32_t workers) {
        delivered_.assign(static_cast<std::size_t>(workers > 0 ? workers : 0), kNone);
        enabled_ = workers > 0;
    }
    [[nodiscard]] bool enabled() const noexcept { return enabled_; }

    /// One control pass, right after the bridge derived the role ops of the metadata below `derivedPos`.
    /// `pendingFor(w)`: role ops for worker w are still waiting for its ring.  `enqueue(w, endNs)`: post
    /// WorkerOp::ReadLease{endNs} on worker w's op ring (false: the ring is full).  Returns the leases posted.
    template <class PendingFn, class EnqueueFn>
    int step(const rune::consensus::ReadLeaseHolder& holder, int64_t derivedPos, PendingFn&& pendingFor,
             EnqueueFn&& enqueue) {
        if (!enabled_) return 0;
        const int64_t best = holder.deliverableEndNs(derivedPos);
        if (best == rune::consensus::ReadLeaseHolder::kNone) return 0;
        int posted = 0;
        for (std::size_t w = 0; w < delivered_.size(); ++w) {
            if (best <= delivered_[w]) continue;
            const auto wid = static_cast<int32_t>(w);
            if (pendingFor(wid)) { ++heldBehindRoleOps_; continue; }
            if (!enqueue(wid, best)) { ++ringFull_; continue; }
            delivered_[w] = best;
            ++posted;
        }
        posted_ += static_cast<uint64_t>(posted);
        return posted;
    }

    [[nodiscard]] int64_t deliveredForTest(int32_t w) const noexcept {
        return w >= 0 && static_cast<std::size_t>(w) < delivered_.size() ? delivered_[static_cast<std::size_t>(w)] : kNone;
    }
    [[nodiscard]] uint64_t postedForTest() const noexcept { return posted_; }
    [[nodiscard]] uint64_t heldBehindRoleOpsForTest() const noexcept { return heldBehindRoleOps_; }
    [[nodiscard]] uint64_t ringFullForTest() const noexcept { return ringFull_; }

    static constexpr int64_t kNone = rune::consensus::ReadLeaseHolder::kNone;

private:
    std::vector<int64_t> delivered_;
    bool     enabled_           = false;
    uint64_t posted_            = 0;
    uint64_t heldBehindRoleOps_ = 0;
    uint64_t ringFull_          = 0;
};

} // namespace rune::node
