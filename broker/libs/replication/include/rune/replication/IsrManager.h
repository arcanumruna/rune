#pragma once
// ---------------------------------------------------------------------------
// IsrManager.h — Pure ISR (in-sync replica set) management for a replicated
//                partition.
//
// Namespace: rune::replication
// C++23, no Aeron dependency, fully unit-testable.
//
// ## Model (spec §4 — in-sync replica set)
//
// The ISR tracks which follower replicas are "in sync" with the leader, i.e.
// not lagging too far behind.  Only ISR members count toward the high-watermark
// majority.  The leader itself is ALWAYS a member of the ISR and is NEVER
// removed.
//
// ## Lag definition (Kafka replica-lag model, spec §4)
//
// A follower is considered LAGGING if ANY of the following holds:
//   1. **Time lag**:  (nowNs - lastFetchTimeNs) > replicaLagMaxMs * 1_000_000
//      (the follower hasn't fetched for longer than the allowed window).
//   2. **Offset lag**: (leaderLeo - lastFetchOffset) > replicaLagMaxOffset
//      (the follower's fetch position is more than the allowed number of
//      offsets behind the leader's log-end-offset).
//
// ## Shrink / Expand
//
// `maybeShrinkIsr`  — remove lagging followers; leader never removed.
// `maybeExpandIsr`  — re-add followers whose lastFetchOffset >= highWatermark
//                     (they have caught up to the committed prefix).
//
// ## Produce gate (min-ISR)
//
// `canProduce(minIsr, acksAll)`:
//   - acks=all ("majority" durability): |ISR| >= minIsr must hold.
//   - acks=leader: always true (leader-only ack, no ISR check).
// ---------------------------------------------------------------------------

#include <algorithm>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include <rune/replication/Types.h>

namespace rune::replication {

/// Per-follower fetch state tracked by IsrManager.
struct FollowerState {
    Offset  lastFetchOffset  = 0;   ///< Last fetchOffset seen from this follower.
    int64_t lastFetchTimeNs  = 0;   ///< Wall-clock time of last fetch (nanoseconds).
};

/// Pure ISR management — tracks replica set membership and per-follower lag,
/// implementing Kafka's replica-lag model (spec §4).
///
/// Thread-safety: NOT thread-safe.  Single-writer per partition (the leader
/// thread owns this object and calls all mutators).
class IsrManager {
public:
    // -----------------------------------------------------------------------
    // Construction / configuration
    // -----------------------------------------------------------------------

    /// Initialise with the full replica set and the leader's NodeId.
    ///
    /// \param replicas   All NodeIds assigned to this partition (including leader).
    /// \param leaderId   The current leader; always kept in ISR.
    ///
    /// On construction the ISR is initialised to contain ALL replicas (they are
    /// assumed in-sync at startup/leader-election time).  Callers may pass an
    /// explicit initial ISR via setIsr() right after construction if a known ISR
    /// is being restored from metadata.
    IsrManager(std::vector<NodeId> replicas, NodeId leaderId)
        : replicas_(std::move(replicas))
        , leaderId_(leaderId)
        , isr_(replicas_)   // start fully in-sync
        , isrChanged_(false)
    {
        // Seed follower state so existing replicas are known.
        for (NodeId n : replicas_) {
            followerState_.try_emplace(n, FollowerState{});
        }
    }

    // -----------------------------------------------------------------------
    // Setters
    // -----------------------------------------------------------------------

    /// Overwrite the ISR (e.g. restored from control-plane metadata).
    void setIsr(std::vector<NodeId> isr) {
        isr_        = std::move(isr);
        isrChanged_ = false; // treat this as a baseline reset
    }

    // -----------------------------------------------------------------------
    // Hot-path state update
    // -----------------------------------------------------------------------

    /// Record a fetch from a follower.
    ///
    /// \param node         The fetching follower's NodeId.
    /// \param fetchOffset  The offset the follower is requesting next (i.e. its
    ///                     current LEO — it holds everything up to fetchOffset-1).
    /// \param nowNs        Current wall-clock time in nanoseconds (injected for
    ///                     deterministic testing).
    void onFollowerFetch(NodeId node, Offset fetchOffset, int64_t nowNs) {
        auto& s = followerState_[node];
        s.lastFetchOffset = fetchOffset;
        s.lastFetchTimeNs = nowNs;
    }

    // -----------------------------------------------------------------------
    // ISR shrink / expand
    // -----------------------------------------------------------------------

    /// Evaluate all ISR members (except the leader) and remove any that are
    /// lagging as of `nowNs`.
    ///
    /// Lag criteria (Kafka replica-lag model, spec §4):
    ///   - Time lag:   nowNs - lastFetchTimeNs > replicaLagMaxMs * 1_000_000
    ///   - Offset lag: leaderLeo - lastFetchOffset > replicaLagMaxOffset
    ///
    /// The leader is NEVER removed.
    ///
    /// \param nowNs               Current wall-clock time in nanoseconds.
    /// \param leaderLeo           Leader's current log-end-offset.
    /// \param replicaLagMaxMs     Max lag in milliseconds before time-expiry.
    /// \param replicaLagMaxOffset Max offset gap before offset-lag expiry.
    /// \returns The new ISR vector (same as isr() after the call).
    std::vector<NodeId> maybeShrinkIsr(int64_t nowNs,
                                       Offset  leaderLeo,
                                       int64_t replicaLagMaxMs,
                                       Offset  replicaLagMaxOffset)
    {
        const int64_t maxLagNs = replicaLagMaxMs * 1'000'000LL;
        std::vector<NodeId> newIsr;
        newIsr.reserve(isr_.size());

        for (NodeId node : isr_) {
            if (node == leaderId_) {
                newIsr.push_back(node); // leader always stays
                continue;
            }
            const auto& s = followerState_[node];
            bool timeLag   = (nowNs - s.lastFetchTimeNs) > maxLagNs;
            bool offsetLag = (leaderLeo - s.lastFetchOffset) > replicaLagMaxOffset;
            if (!timeLag && !offsetLag) {
                newIsr.push_back(node);
            }
            // lagging: omit from newIsr (shrunk out)
        }

        isrChanged_ = (newIsr != isr_);
        isr_        = std::move(newIsr);
        return isr_;
    }

    /// Evaluate all replicas NOT currently in the ISR and add back any that
    /// have caught up: lastFetchOffset >= highWatermark.
    ///
    /// \param nowNs         Current wall-clock time in nanoseconds (reserved for
    ///                      future time-based gating; not applied in this version
    ///                      because catching up to HW is the primary criterion).
    /// \param highWatermark The partition high-watermark (committed prefix).
    /// \returns The new ISR vector (same as isr() after the call).
    std::vector<NodeId> maybeExpandIsr(int64_t /*nowNs*/, Offset highWatermark) {
        std::vector<NodeId> newIsr = isr_;

        for (NodeId node : replicas_) {
            if (inIsr(node)) continue; // already in ISR

            const auto& s = followerState_[node];
            if (s.lastFetchOffset >= highWatermark) {
                newIsr.push_back(node);
            }
        }

        isrChanged_ = (newIsr != isr_);
        isr_        = std::move(newIsr);
        return isr_;
    }

    // -----------------------------------------------------------------------
    // Produce gate
    // -----------------------------------------------------------------------

    /// Return true iff the partition may accept a produce request.
    ///
    /// \param minIsr   Minimum ISR size required for durable acks.
    /// \param acksAll  true → majority-durable ("all"/"majority"); the ISR
    ///                 size must meet minIsr.
    ///                 false → leader-only ack; always allowed.
    [[nodiscard]] bool canProduce(int minIsr, bool acksAll) const noexcept {
        if (!acksAll) return true;
        return static_cast<int>(isr_.size()) >= minIsr;
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// The current in-sync replica set.
    [[nodiscard]] const std::vector<NodeId>& isr() const noexcept { return isr_; }

    /// True if `node` is currently in the ISR.
    [[nodiscard]] bool inIsr(NodeId node) const noexcept {
        return std::find(isr_.begin(), isr_.end(), node) != isr_.end();
    }

    /// True if the last maybeShrinkIsr() or maybeExpandIsr() call changed the ISR.
    [[nodiscard]] bool isrChanged() const noexcept { return isrChanged_; }

    /// The full replica set (ISR + out-of-sync replicas).
    [[nodiscard]] const std::vector<NodeId>& replicas() const noexcept { return replicas_; }

    /// Per-follower fetch state (for inspection / testing).
    [[nodiscard]] const FollowerState* followerState(NodeId node) const noexcept {
        auto it = followerState_.find(node);
        return (it != followerState_.end()) ? &it->second : nullptr;
    }

private:
    NodeId                              leaderId_;
    std::vector<NodeId>                 replicas_;
    std::vector<NodeId>                 isr_;
    std::unordered_map<NodeId, FollowerState> followerState_;
    bool                                isrChanged_ = false;
};

} // namespace rune::replication
