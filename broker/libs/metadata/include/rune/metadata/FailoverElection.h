#pragma once
// ---------------------------------------------------------------------------
// FailoverElection.h — position/lineage-aware failover election.
//
// Kafka parity §3, §7 + Rune Milestone D Phase-2 design §D.
//
// Algorithm (pure function — no I/O, no clock, no shared mutable state):
//   1. Keep current leader if still live AND in cur.isr (min-churn).
//   2. Compute PROPOSED isr = cur.isr ∩ liveNodes (re-run: never trust raw ISR).
//   3. Among proposed members pick the most-complete by
//      compareLog(leaderEpoch, leo) — higher leaderEpoch wins, then higher leo;
//      a member with no position report contributes a sentinel {epoch=-1,leo=-1}.
//      Tie-break: lowest nodeId.
//   4. NO LEADER if proposed isr is empty (no ISR survivor).
//   5. newIsr = {elected leader, ...other proposed members}.
//
// TWO-EPOCH NOTE: leaderEpoch bumps only on leader-change or NO_LEADER
//   (delegated to mergeChange — never falsely bumped here).
//
// CARRY-FORWARD: ElectionInputs::liveNodes MUST be populated from
//   MembershipState::eligibleNodes() (Unfenced ∧ caughtUp ∧ ¬expired) by the
//   caller.  A just-joined / not-yet-caught-up node is NOT eligible per Kafka §8.
//   FailoverElection itself is a pure computation over the supplied set; it does
//   not read MembershipState directly so tests can pass any node list.
//
// STALE-READ TOLERANCE: the completeness gate (re-run over proposed ISR only)
//   plus the epoch-primary / leo-secondary ordering of compareLog means a
//   momentarily-stale ReplicaPositionTable entry can only cause election of a
//   slightly-older (but still safe) replica, never an unsafe one from outside the
//   ISR lineage.
// ---------------------------------------------------------------------------

#include <rune/metadata/PartitionAssignment.h>
#include <rune/metadata/ReplicaPositionTable.h>
#include <rune/consensus/ClusterMember.h>  // compareLog(Term,int64_t,Term,int64_t)
#include <cstdint>
#include <vector>
#include <algorithm>

namespace rune::metadata {

// ---------------------------------------------------------------------------
// ElectionInputs — everything elect() needs; no external I/O.
// ---------------------------------------------------------------------------
struct ElectionInputs {
    PartitionAssignment       cur;          ///< Current committed assignment.
    std::vector<int32_t>      liveNodes;    ///< Caller MUST supply eligibleNodes().
    ReplicaPositionTable*     positions;    ///< Soft position table (may be stale).
    bool                      allowUnclean = false; ///< reserved; currently unused.
};

// ---------------------------------------------------------------------------
// ElectionOutcome — result of one election pass.
// ---------------------------------------------------------------------------
struct ElectionOutcome {
    AssignmentChange change;       ///< Apply via mergeChange(cur, change).
    bool             leaderChanged = false;
    bool             wentNoLeader  = false;
};

// ---------------------------------------------------------------------------
// FailoverElection — stateless pure election logic.
// ---------------------------------------------------------------------------
class FailoverElection {
public:
    /// Run a failover election for (topicId, partition).
    ///
    /// \param topicId    Topic identifier (used to key into the position table).
    /// \param partition  Partition index.
    /// \param in         Election inputs (see ElectionInputs).
    /// \returns          ElectionOutcome describing the resulting AssignmentChange.
    static ElectionOutcome elect(int32_t topicId, int32_t partition,
                                 const ElectionInputs& in) noexcept
    {
        const PartitionAssignment& cur = in.cur;

        // ── Rule 1: keep current leader if still live AND in cur.isr ──────────
        // "live" = present in liveNodes; "in ISR" = present in cur.isr.
        const bool currentLeaderLive = isIn(cur.leaderNode, in.liveNodes);
        const bool currentLeaderInIsr = (cur.leaderNode != NO_LEADER)
                                      && isIn(cur.leaderNode, cur.isr);

        if (currentLeaderLive && currentLeaderInIsr) {
            // Min-churn: no change needed.
            ElectionOutcome out;
            out.leaderChanged = false;
            out.wentNoLeader  = false;
            // Sentinel no-op change.
            out.change.newLeader   = NO_LEADER_CHANGE;
            out.change.newIsr      = {};
            out.change.newReplicas = {};
            return out;
        }

        // ── Rule 2 + 3: compute PROPOSED isr = cur.isr ∩ liveNodes ──────────
        // Re-run over proposed ISR — never trust the raw cur.isr.
        std::vector<int32_t> proposedIsr;
        proposedIsr.reserve(cur.isr.size());
        for (int32_t node : cur.isr) {
            if (isIn(node, in.liveNodes)) {
                proposedIsr.push_back(node);
            }
        }

        // ── Rule 4: no ISR survivor → NO_LEADER ─────────────────────────────
        if (proposedIsr.empty()) {
            ElectionOutcome out;
            out.leaderChanged = true;
            out.wentNoLeader  = true;
            out.change.newLeader   = NO_LEADER;
            out.change.newIsr      = {};
            out.change.newReplicas = {};  // empty → mergeChange keeps cur.replicas
            return out;
        }

        // ── Rule 2: pick the most-complete proposed member ───────────────────
        // Sentinel for a member with no position report: epoch=-1, leo=-1.
        // compareLog(lhsEpoch, lhsLeo, rhsEpoch, rhsLeo): +1 if lhs more recent.
        // Tie-break: lowest nodeId (stable sort order).

        auto getPos = [&](int32_t node)
            -> std::pair<rune::consensus::Term, int64_t>
        {
            auto opt = in.positions ? in.positions->get(topicId, partition, node)
                                    : std::nullopt;
            if (!opt.has_value()) return { -1LL, -1LL };
            return { static_cast<rune::consensus::Term>(opt->leaderEpoch),
                     opt->logEndPosition };
        };

        int32_t elected = proposedIsr[0];
        auto [bestEpoch, bestLeo] = getPos(elected);

        for (std::size_t i = 1; i < proposedIsr.size(); ++i) {
            int32_t candidate = proposedIsr[i];
            auto [cEpoch, cLeo] = getPos(candidate);

            int cmp = rune::consensus::ClusterMember::compareLog(
                cEpoch, cLeo, bestEpoch, bestLeo);

            if (cmp > 0) {
                // Candidate is more complete.
                elected   = candidate;
                bestEpoch = cEpoch;
                bestLeo   = cLeo;
            } else if (cmp == 0 && candidate < elected) {
                // Equal position: prefer lowest nodeId (fence-one-at-a-time stability).
                elected = candidate;
                // bestEpoch/bestLeo unchanged (equal)
            }
        }

        // ── Rule 5: newIsr = {elected, ...other proposed} ────────────────────
        // Leader is always first in ISR.
        std::vector<int32_t> newIsr;
        newIsr.reserve(proposedIsr.size());
        newIsr.push_back(elected);
        for (int32_t node : proposedIsr) {
            if (node != elected) newIsr.push_back(node);
        }

        ElectionOutcome out;
        out.leaderChanged        = true;
        out.wentNoLeader         = false;
        out.change.newLeader     = elected;
        out.change.newIsr        = std::move(newIsr);
        out.change.newReplicas   = {};  // empty → mergeChange keeps cur.replicas
        return out;
    }

private:
    /// Linear membership check — ISR/replica sets are small (RF ≤ 9).
    static bool isIn(int32_t node, const std::vector<int32_t>& set) noexcept {
        for (int32_t x : set) if (x == node) return true;
        return false;
    }
};

} // namespace rune::metadata
