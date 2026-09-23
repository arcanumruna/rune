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
//        – Follower: the fetchOffset MOST RECENTLY reported in its FetchRequest
//          (updated via updateFollowerFetchOffset()).  Unknown followers
//          default to 0 (conservative: they haven't confirmed anything).
//   2. Sort LEOs descending.
//   3. majority = |ISR| / 2 + 1  (strict majority).
//   4. HW = LEOs[majority - 1]  (the majority-th highest = the value that
//      at least `majority` members have reached or exceeded).
//   5. HW is monotonic: store max(prevHW, computed).  Once advanced, it
//      never regresses even if a follower truncates and re-fetches from a
//      lower offset — the per-follower LEO tracks the LATEST reported value
//      (may decrease on truncation), but the global HW floor prevHw_ never
//      goes below the highest committed point.
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
// ## Per-follower LEO semantics
//
// updateFollowerFetchOffset() tracks the LATEST reported fetch offset per
// follower — it is NOT monotonic per-follower.  When a follower truncates and
// re-fetches from a lower offset, the leader must use the lower value so HW
// is recomputed from what a majority CURRENTLY holds, not from a stale
// pre-truncation value.  The GLOBAL HW floor (prevHw_) remains monotonic:
// once HW reaches X it never drops below X, preserving the committed prefix
// guarantee.
//
// ## Recomputation
//
// highWatermark() always returns the monotonic floor prevHw_, which is
// recomputed eagerly inside every mutator (setLeaderLogEndOffset, setIsr,
// updateFollowerFetchOffset).  Callers do not need to call a separate
// recompute() method.
// ---------------------------------------------------------------------------

#include <algorithm>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include <rune/replication/Types.h>

namespace rune::replication {

/// Per-topic durability quorum rule.
///   Majority  — HW advances when a MAJORITY of the ISR holds the batch (spec §4 default).
///   AllInSync — HW advances only when EVERY ISR member holds the batch (full-ISR min).
enum class DurabilityMode : uint8_t { Majority = 0, AllInSync = 1 };

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
    /// Eagerly recomputes and advances the monotonic HW floor so that
    /// highWatermark() is never stale after a membership change.
    void setIsr(std::vector<NodeId> isr) {
        isr_ = std::move(isr);
        recomputeHw(); // S1: recompute on every mutator
    }

    // -----------------------------------------------------------------------
    // State updates (hot path — called by the leader on every append/fetch)
    // -----------------------------------------------------------------------

    /// Advance the leader's own logEndOffset.
    /// Called after each successful append to the leader's in-memory log.
    /// Eagerly recomputes HW so highWatermark() is never stale.
    void setLeaderLogEndOffset(Offset leo) noexcept {
        leaderLeo_ = leo;
        recomputeHw(); // S1: recompute on every mutator
    }

    /// Record that a follower has confirmed it has fetched up to `fetchOffset`.
    ///
    /// `fetchOffset` in a FetchRequest is the follower's logEndOffset
    /// (the next offset it wants), which means it holds all records up to
    /// fetchOffset - 1.  For HW purposes, we treat it as the follower's LEO.
    ///
    /// Per-follower LEO tracks the LATEST (most recently) reported value and
    /// is NOT monotonically clamped.  When a follower truncates and re-fetches
    /// from a lower offset, the lower value must be recorded so HW is
    /// recomputed from what the follower CURRENTLY holds — not a stale
    /// pre-truncation value that could cause HW to advance past data only a
    /// pre-truncation minority held (B1 fix).
    ///
    /// The GLOBAL HW floor (prevHw_) remains monotonic: once committed, HW
    /// never regresses, so the committed prefix is never exposed as
    /// uncommitted.
    void updateFollowerFetchOffset(NodeId followerId, Offset fetchOffset) {
        followerLeo_[followerId] = fetchOffset; // B1 fix: SET not max
        // Recompute and advance the monotonic committed floor.
        recomputeHw();
    }

    /// Set the durability quorum rule for this leader state.
    /// Majority (default) — HW = majority-th highest LEO.
    /// AllInSync — HW = full-ISR minimum LEO.
    ///
    /// Changing the mode resets the monotonic HW floor and recomputes from the
    /// current ISR LEOs under the new rule.  This is safe because setDurabilityMode
    /// is only called from becomeLeader (a role transition), where the floor is
    /// re-seeded by setHighWatermarkFloor() immediately before this call.  It must
    /// NOT be called on a live leader without first re-seeding the floor.
    void setDurabilityMode(DurabilityMode m) noexcept {
        durability_ = m;
        prevHw_  = 0;  // reset floor so the new rule computes from scratch
        hwFloor_ = 0;  // reset hard floor (will be re-seeded by setHighWatermarkFloor)
        recomputeHw();
    }

    /// Expose a public recompute trigger for callers that need to force an
    /// HW recalculation (e.g., after setLeaderLogEndOffset when the ISR is
    /// leader-only and no follower fetch triggers recomputation).
    void recompute() noexcept { recomputeHw(); }

    /// Set the initial high-watermark floor (B2 fix: called by becomeLeader to
    /// restore the committed offset so HW never regresses to 0 on failover).
    ///
    /// For Majority mode: advances prevHw_ monotonically.
    /// For AllInSync mode: records a hard floor so that even a non-monotonic
    ///   AllInSync recompute cannot go below the pre-failover committed offset.
    void setHighWatermarkFloor(Offset floor) noexcept {
        if (floor > hwFloor_) hwFloor_ = floor;
        if (floor > prevHw_) prevHw_ = floor;
    }

    // -----------------------------------------------------------------------
    // Queries
    // -----------------------------------------------------------------------

    /// Return the majority high-watermark (spec §4).
    ///
    /// Returns the monotonic floor prevHw_, which is eagerly updated by every
    /// mutator (setLeaderLogEndOffset, setIsr, updateFollowerFetchOffset).
    /// The value is always current — no separate recompute() call required.
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
    // Internal: recompute HW and update the floor.
    //
    // Majority (default): prevHw_ is MONOTONIC — only advances.
    //   A follower truncation does not lower the committed frontier because
    //   a majority already holds the batch (spec §4).
    //
    // AllInSync: prevHw_ tracks the TRUE ISR minimum — it CAN retract when a
    //   follower falls behind.  This is intentional: the AllInSync guarantee is
    //   "every ISR member holds the batch", so if any member regresses the HW
    //   must reflect that.  Consumers using AllInSync topics must tolerate a
    //   non-monotonic HW (the topic is configured for stricter durability, not
    //   strict delivery monotonicity at the HW level).
    // -----------------------------------------------------------------------
    void recomputeHw() noexcept {
        Offset computed = computeHw();
        if (durability_ == DurabilityMode::AllInSync) {
            // AllInSync: track the true ISR minimum; may retract on follower lag,
            // but never below the hard floor set by setHighWatermarkFloor (the
            // pre-failover committed offset on becomeLeader).
            prevHw_ = (computed > hwFloor_) ? computed : hwFloor_;
        } else {
            // Majority: monotonic floor — only advance, never retract.
            if (computed > prevHw_) {
                prevHw_ = computed;
            }
        }
    }

    Offset computeHw() const noexcept {
        if (isr_.empty()) return 0;

        const int isrSize  = static_cast<int>(isr_.size());
        const int majority = isrSize / 2 + 1; // strict majority, e.g. 3→2, 5→3

        // A-NEW-2 fix: use a dynamically-sized buffer so ISR sizes > 32
        // (e.g. 33, 64, 65) are computed correctly without OOB stack reads.
        //
        // For the common ISR sizes (3–5) this still avoids heap allocation via
        // the small-buffer path: we allocate on the stack for ISR <= kSmallIsr
        // and fall back to a heap vector for larger sizes.
        //
        // Prior bug: kMaxIsr=32 and the array was filled only up to
        // n=min(isrSize,32), but majority was computed from the FULL isrSize.
        // For isrSize>=64, majority-1>=32 indexed past the 32-element array
        // (UB); for 33<=isrSize<=63 it read a zero-initialised slot (HW
        // under-computed, not UB).  Now majority is clamped to n so the two
        // always agree.
        constexpr int kSmallIsr = 32;
        Offset smallBuf[kSmallIsr]{};
        std::vector<Offset> heapBuf;
        Offset* leos = nullptr;

        if (isrSize <= kSmallIsr) {
            leos = smallBuf;
        } else {
            heapBuf.assign(static_cast<std::size_t>(isrSize), Offset{0});
            leos = heapBuf.data();
        }

        const int n = isrSize; // always use ALL ISR members

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

        // Select the HW index based on the durability mode:
        //   Majority  → majority-th highest (leos[majority - 1])
        //   AllInSync → full-ISR minimum    (leos[n - 1])
        // Both majority-1 and n-1 are always in [0, n-1], so always in-bounds.
        // E.g. ISR={A=100, B=90, C=80}, Majority: majority=2, HW=leos[1]=90.
        //                               AllInSync: idx=2,      HW=leos[2]=80.
        const int idx = (durability_ == DurabilityMode::AllInSync)
            ? (n - 1)             // full-ISR min (last after descending sort)
            : (majority - 1);     // majority-th highest
        return leos[idx];
    }

    NodeId       leaderId_   = -1;
    Offset       leaderLeo_ = 0;
    Offset       prevHw_    = 0;   ///< HW floor (monotonic for Majority; ISR-min for AllInSync, floored by hwFloor_).
    Offset       hwFloor_   = 0;   ///< Hard floor for AllInSync mode: HW never goes below the pre-failover committed offset.
    DurabilityMode durability_ = DurabilityMode::Majority; ///< Per-topic HW quorum rule.

    std::vector<NodeId>            isr_;          ///< Current in-sync replica set.
    std::unordered_map<NodeId, Offset> followerLeo_; ///< Per-follower LEO.
};

} // namespace rune::replication
