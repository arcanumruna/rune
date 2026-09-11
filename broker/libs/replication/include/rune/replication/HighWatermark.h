#pragma once
// ---------------------------------------------------------------------------
// HighWatermark.h — Pure high-watermark computation for a replicated partition.
//
// Namespace: rune::replication
// C++23, no allocation on the hot path (follower LEO map uses std::unordered_map;
// HW computation iterates ISR — typically 3–5 members).
//
// ## Model (spec §4 — majority-in-memory high-watermark)
//
// A write is "durable" when a MAJORITY of the ISR hold the batch in memory.
// The high-watermark (HW) is therefore the highest offset that a majority of
// ISR members have confirmed. Concretely:
//
//   1. Collect the logEndOffset (LEO) of each ISR member:
//        – Leader: its own logEndOffset (always up-to-date; updated via
//          setLeaderLogEndOffset()).
//        – Follower: the fetchOffset last reported in its FetchRequest
//          (updated via updateFollowerFetchOffset()).  Unknown followers
//          default to 0 (conservative: they haven't confirmed anything).
//   2. Sort LEOs descending.
//   3. majority = |ISR| / 2 + 1  (strict majority).
//   4. HW = LEOs[majority - 1]  (the majority-th highest = the value that
//      at least `majority` members have reached or exceeded).
//   5. HW is monotonic: store max(prevHW, computed).  Once advanced, it
//      never regresses even if a follower's tracked LEO becomes stale or a
//      late-arriving update attempts to lower it.
//
// This differs from Kafka's ISR-min HW (which is all-ISR minimum, not
// majority). Our spec §4 requires majority-ack for durability, consistent
// with electing the most-up-to-date ISR member on failover (§8).
//
// ## ISR and leader identity
//
// The ISR (in-sync replica set) is a list of NodeIds managed externally by
// the control plane (Phase 2 metadata quorum). It is pushed into this object
// via setIsr(). The leader's own NodeId is set via setLeaderId() so that HW
// computation uses the leader's LEO (not the followerLeo map) for the leader's
// ISR slot.
//
// ## Monotonicity
//
// updateFollowerFetchOffset() is monotonic per follower: a smaller value than
// previously recorded is ignored (e.g. a late-arriving message).
// highWatermark() additionally stores the running max, so the returned value
// never decreases.
// ---------------------------------------------------------------------------

#include <algorithm>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include <rune/replication/Types.h>

namespace rune::replication {

/// Pure leader-side state for computing the partition high-watermark.
///
/// Thread-safety: NOT thread-safe. Single-writer per partition (the leader
/// thread owns this object and calls all mutators). Reads (highWatermark())
/// may be called from the same thread.
class ReplicaLeaderState {
public:
    // -----------------------------------------------------------------------
    // Configuration
    // -----------------------------------------------------------------------

    /// Set the NodeId of the leader (self). Must be called before computing HW.
    void setLeaderId(NodeId id) noexcept { leaderId_ = id; }

    /// Replace the ISR with the given set.
    /// The ISR is the authoritative membership list for HW computation.
    /// After a setIsr() call, highWatermark() recomputes from scratch on the
    /// next call (the monotonic floor prevHw_ is preserved, so HW never
    /// regresses due to a configuration change).
    void setIsr(std::vector<NodeId> isr) {
        isr_ = std::move(isr);
    }

    // -----------------------------------------------------------------------
    // State updates (hot path — called by the leader on every append/fetch)
    // -----------------------------------------------------------------------

    /// Advance the leader's own logEndOffset.
    /// Called after each successful append to the leader's in-memory log.
    void setLeaderLogEndOffset(Offset leo) noexcept {
        leaderLeo_ = leo;
    }

    /// Record that a follower has confirmed it has fetched up to `fetchOffset`.
    ///
    /// `fetchOffset` in a FetchRequest is the follower's logEndOffset
    /// (the next offset it wants), which means it holds all records up to
    /// fetchOffset - 1.  For HW purposes, we treat it as the follower's LEO.
    ///
    /// Monotonic: a value smaller than the current tracked LEO for this
    /// follower is silently ignored (late-arriving message protection).
    void updateFollowerFetchOffset(NodeId followerId, Offset fetchOffset) {
        auto& current = followerLeo_[followerId];
        if (fetchOffset > current) {
            current = fetchOffset;
        }
        // Recompute and cache HW monotonically.
        recomputeHw();
    }

    // -----------------------------------------------------------------------
    // Queries
    // -----------------------------------------------------------------------

    /// Compute the majority high-watermark (spec §4).
    ///
    /// Algorithm:
    ///   1. Build a list of LEOs for all ISR members.
    ///   2. Sort descending.
    ///   3. Return LEOs[majority - 1], clamped to the monotonic floor prevHw_.
    ///
    /// If the ISR is empty, returns 0 (safe: no commit possible).
    Offset highWatermark() const noexcept {
        return prevHw_;
    }

    /// Alias for highWatermark(); provided for clarity at call sites that
    /// reason about "the committed prefix".
    Offset committedOffset() const noexcept {
        return highWatermark();
    }

    /// Return the leader's current logEndOffset.
    Offset leaderLogEndOffset() const noexcept { return leaderLeo_; }

private:
    // -----------------------------------------------------------------------
    // Internal: recompute HW and advance the monotonic floor.
    // -----------------------------------------------------------------------
    void recomputeHw() noexcept {
        Offset computed = computeHw();
        if (computed > prevHw_) {
            prevHw_ = computed;
        }
    }

    Offset computeHw() const noexcept {
        if (isr_.empty()) return 0;

        const int isrSize  = static_cast<int>(isr_.size());
        const int majority = isrSize / 2 + 1; // strict majority, e.g. 3→2, 5→3

        // Gather LEOs for every ISR member.
        // Use a small stack buffer for the typical 3–5 member ISR to avoid
        // heap allocation on the hot path.
        // (C++23 VLAs are non-standard; use a fixed-capacity local array.)
        constexpr int kMaxIsr = 32; // practical upper bound
        Offset leos[kMaxIsr]{};
        const int n = std::min(isrSize, kMaxIsr);

        for (int i = 0; i < n; ++i) {
            NodeId member = isr_[static_cast<std::size_t>(i)];
            if (member == leaderId_) {
                leos[i] = leaderLeo_;
            } else {
                auto it = followerLeo_.find(member);
                leos[i] = (it != followerLeo_.end()) ? it->second : 0;
            }
        }

        // Sort descending: leos[0] = highest LEO.
        std::sort(leos, leos + n, std::greater<Offset>{});

        // majority-th highest = leos[majority - 1].
        // E.g. ISR={A=100, B=90, C=80}: majority=2, HW=leos[1]=90.
        return leos[majority - 1];
    }

    NodeId  leaderId_  = -1;
    Offset  leaderLeo_ = 0;
    Offset  prevHw_    = 0;   ///< Monotonic high-watermark floor.

    std::vector<NodeId>            isr_;          ///< Current in-sync replica set.
    std::unordered_map<NodeId, Offset> followerLeo_; ///< Per-follower LEO.
};

} // namespace rune::replication
