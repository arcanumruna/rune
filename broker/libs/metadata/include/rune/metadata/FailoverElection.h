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
    /// Set when an unclean election fires: a non-ISR replica was elected,
    /// ISR was forced to {newLeader}, and committed data may have been lost.
    /// Only ever true when ElectionInputs::allowUnclean == true.
    bool             recovering    = false;
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

        // ── getPos helper — shared by clean and unclean paths ────────────────
        // Sentinel for a member with no position report: epoch=-1, leo=-1.
        // compareLog(lhsEpoch, lhsLeo, rhsEpoch, rhsLeo): +1 if lhs more recent.
        auto getPos = [&](int32_t node)
            -> std::pair<rune::consensus::Term, int64_t>
        {
            auto opt = in.positions ? in.positions->get(topicId, partition, node)
                                    : std::nullopt;
            if (!opt.has_value()) return { -1LL, -1LL };
            return { static_cast<rune::consensus::Term>(opt->leaderEpoch),
                     opt->logEndPosition };
        };

        // ── Rule 4: no ISR survivor → NO_LEADER (or unclean if opted-in) ───────
        if (proposedIsr.empty()) {
            // ── Unclean opt-in: per-topic override, default OFF ───────────────
            // ONLY fires when allowUnclean==true AND there is at least one live
            // replica in cur.replicas (ignoring ISR membership). This is the
            // SOLE place the no-committed-loss invariant is intentionally
            // relaxed. Pick the most-complete live replica, force ISR={newLeader},
            // and mark recovering=true.
            if (in.allowUnclean) {
                // Collect live replicas (cur.replicas ∩ liveNodes), ignoring ISR.
                std::vector<int32_t> liveCandidates;
                liveCandidates.reserve(cur.replicas.size());
                for (int32_t node : cur.replicas) {
                    if (isIn(node, in.liveNodes)) {
                        liveCandidates.push_back(node);
                    }
                }

                if (!liveCandidates.empty()) {
                    // Pick the most-complete live replica by compareLog;
                    // tie-break: lowest nodeId (same ordering as clean path).
                    int32_t uncleanLeader = liveCandidates[0];
                    auto [bestU, bestULeo] = getPos(uncleanLeader);

                    for (std::size_t i = 1; i < liveCandidates.size(); ++i) {
                        int32_t candidate = liveCandidates[i];
                        auto [cEpoch, cLeo] = getPos(candidate);

                        int cmp = rune::consensus::ClusterMember::compareLog(
                            cEpoch, cLeo, bestU, bestULeo);

                        if (cmp > 0) {
                            uncleanLeader = candidate;
                            bestU         = cEpoch;
                            bestULeo      = cLeo;
                        } else if (cmp == 0 && candidate < uncleanLeader) {
                            uncleanLeader = candidate;
                        }
                    }

                    ElectionOutcome out;
                    out.leaderChanged        = true;
                    out.wentNoLeader         = false;
                    out.recovering           = true;
                    out.change.newLeader     = uncleanLeader;
                    out.change.newIsr        = { uncleanLeader };  // forced single-member ISR
                    out.change.newReplicas   = {};  // empty → mergeChange keeps cur.replicas
                    return out;
                }
            }

            // Default (allowUnclean==false, or no live replicas): unavailable.
            ElectionOutcome out;
            out.leaderChanged = true;
            out.wentNoLeader  = true;
            out.change.newLeader   = NO_LEADER;
            out.change.newIsr      = {};
            out.change.newReplicas = {};  // empty → mergeChange keeps cur.replicas
            return out;
        }

        // ── Rule 2: pick the most-complete proposed member ───────────────────
        // Tie-break: lowest nodeId (stable sort order).

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
