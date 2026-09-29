// DynamicBalancer.cpp — delta rebalance engine implementation.
//
// Algorithm:
//   1. Build ReverseIndex from current AssignmentMap.
//   2. Count current leader-load per node to detect imbalance.
//   3. Group topics by coPartitionGroup (non-empty) or treat as singletons.
//   4. For each group (or singleton) + each partition index:
//      a. Determine if any topic in the group has a dead leader OR wants a
//         preferred-leader election (replicas[0] ∈ ISR, would improve balance).
//      b. For dead-leader: run FailoverElection::elect() on the first topic;
//         use its elected leader for ALL topics in the group (co-partition unit move).
//      c. For preferred-leader (replicas[0] ∈ ISR, live, not current leader,
//         budget > 0, and would reduce imbalance): elect replicas[0].
//      d. mergeChange() to compute the next PartitionAssignment.
//      e. If the result differs from cur (leader or ISR changed), emit a BalancerRecord.
//   5. Skip no-ops (mergeChange returns same state → partitionEpoch not bumped).
//
// CARRY-FORWARD constraints:
//   - liveNodes = eligibleNodes() (caller responsibility; we compute on that set).
//   - Preferred leader = replicas[0] ONLY if replicas[0] ∈ cur.isr.
//   - Epoch bumps entirely delegated to mergeChange().
//
// PREFERRED-LEADER ELECTION POLICY:
//   Preferred-leader elections only fire when they IMPROVE the leadership balance:
//   the preferred leader (replicas[0]) must lead FEWER partitions than the current
//   leader before the swap.  This ensures min-churn in steady state: an evenly-
//   distributed assignment never triggers unnecessary leadership changes.

#include "rune/metadata/DynamicBalancer.h"
#include "rune/metadata/FailoverElection.h"
#include "rune/metadata/Gpid.h"

#include <algorithm>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

namespace rune::metadata {

// ============================================================================
// ReverseIndex implementation
// ============================================================================

namespace {

inline std::pair<int32_t,int32_t> wordAndBit(int32_t gpid) noexcept {
    return { gpid / ReverseIndex::kGpidsPerWord,
             gpid % ReverseIndex::kGpidsPerWord };
}

} // anonymous namespace

void ReverseIndex::setBit(int32_t node, int32_t gpid)
{
    auto& bits = nodeIndex_[node];
    auto [word, bit] = wordAndBit(gpid);
    if (static_cast<int32_t>(bits.size()) <= word)
        bits.resize(static_cast<std::size_t>(word + 1), 0u);
    bits[static_cast<std::size_t>(word)] |= (uint64_t{1} << bit);
}

void ReverseIndex::clearBit(int32_t node, int32_t gpid)
{
    auto it = nodeIndex_.find(node);
    if (it == nodeIndex_.end()) return;
    auto [word, bit] = wordAndBit(gpid);
    if (static_cast<int32_t>(it->second.size()) <= word) return;
    it->second[static_cast<std::size_t>(word)] &= ~(uint64_t{1} << bit);
}

bool ReverseIndex::testBit(int32_t node, int32_t gpid) const
{
    auto it = nodeIndex_.find(node);
    if (it == nodeIndex_.end()) return false;
    auto [word, bit] = wordAndBit(gpid);
    if (static_cast<int32_t>(it->second.size()) <= word) return false;
    return (it->second[static_cast<std::size_t>(word)] >> bit) & 1u;
}

void ReverseIndex::rebuild(const AssignmentMap& m)
{
    nodeIndex_.clear();
    m.forEach([this](int32_t topicId, int32_t partition,
                     const PartitionAssignment& pa)
    {
        if (pa.leaderNode == NO_LEADER) return;
        setBit(pa.leaderNode, gpidOf(topicId, partition));
    });
}

std::vector<std::pair<int32_t,int32_t>>
ReverseIndex::partitionsLedBy(int32_t node) const
{
    std::vector<std::pair<int32_t,int32_t>> result;
    auto it = nodeIndex_.find(node);
    if (it == nodeIndex_.end()) return result;

    const auto& bits = it->second;
    for (int32_t w = 0; w < static_cast<int32_t>(bits.size()); ++w) {
        uint64_t word = bits[static_cast<std::size_t>(w)];
        while (word != 0) {
            int32_t bit = __builtin_ctzll(word);
            const int32_t gpid = w * kGpidsPerWord + bit;
            const int32_t topicId   = gpid / kMaxGpidPartitions;
            const int32_t partition = gpid % kMaxGpidPartitions;
            result.emplace_back(topicId, partition);
            word &= word - 1;  // clear lowest set bit
        }
    }
    return result;
}

void ReverseIndex::onAssign(int32_t topicId, int32_t partition,
                            int32_t oldLeader, int32_t newLeader)
{
    const int32_t gpid = gpidOf(topicId, partition);
    if (oldLeader != NO_LEADER) clearBit(oldLeader, gpid);
    if (newLeader != NO_LEADER) setBit(newLeader, gpid);
}

// ============================================================================
// Helper: build an Int32ArrayView from a std::vector<int32_t>
// (mirrors StaticBalancer convention — LE host assumed per project policy)
// ============================================================================
static rune::protocol::metadata::Int32ArrayView makeView(const std::vector<int32_t>& v)
{
    return rune::protocol::metadata::Int32ArrayView(
        v.data(),
        static_cast<uint16_t>(v.size()));
}

// ============================================================================
// Helper: is `node` present in `vec`?  Linear scan (sets are small, RF ≤ 9).
// ============================================================================
static bool isIn(int32_t node, const std::vector<int32_t>& vec) noexcept
{
    for (int32_t x : vec) if (x == node) return true;
    return false;
}

// ============================================================================
// DynamicBalancer::rebalance
// ============================================================================

std::vector<BalancerRecord> DynamicBalancer::rebalance(const DynamicBalancerInputs& in)
{
    if (!in.cur || in.topics.empty()) return {};

    // ── 1. Build reverse index ──────────────────────────────────────────────
    ReverseIndex idx;
    idx.rebuild(*in.cur);

    // ── 2. Compute current leader-load per live node ─────────────────────────
    //    This is used for the preferred-leader balance check: we only trigger
    //    preferred-leader elections when they strictly improve balance (the
    //    preferred node leads fewer partitions than the current leader).
    //    Using the ReverseIndex for O(nodes * avg_partitions_per_node).
    std::unordered_map<int32_t, int32_t> leaderLoad;
    for (int32_t node : in.liveNodes) {
        leaderLoad[node] = static_cast<int32_t>(idx.partitionsLedBy(node).size());
    }
    // Running load table: updated as we emit records so later decisions
    // reflect already-decided swaps within this rebalance cycle.
    // (Mutable copy; we adjust when we emit a leadership change.)

    // ── Restorability check (time-independent convergence) ────────────────────
    // A live replica is "restorable" — safe to re-admit to the ISR / re-elect as
    // preferred leader after a transient-liveness collapse dropped it from the ISR —
    // ONLY when we have POSITIVE proof it still holds the committed prefix: its
    // reported logEndPosition >= the reference leader's, from the ReplicaPositionTable.
    //
    // Absent that proof (no position report for the node or the leader) we must NOT
    // restore: a node outside the ISR is by-definition not in-sync, and electing it
    // without proof of catch-up would risk committed-data loss.  This keeps the old
    // "preferred-leader only if in ISR" safety while ADDING position-proven
    // restoration — the mechanism that lets a transient heartbeat false-positive
    // stop permanently collapsing leadership, for ANY window (no tuned grace).
    //
    // NOTE: this activates only once replica position reports are plumbed into the
    // controller's ReplicaPositionTable; until then it is a safe no-op (see the
    // position-report wiring in NodeRuntimeAgent / MetadataService).
    auto restorable = [&](int32_t topicId, int32_t part,
                          int32_t node, int32_t refLeader) -> bool {
        if (!in.positions) return false;
        const auto np = in.positions->get(topicId, part, node);
        const auto lp = in.positions->get(topicId, part, refLeader);
        if (!np || !lp) return false;   // no proof of catch-up → not restorable (safety)
        return np->logEndPosition >= lp->logEndPosition;
    };

    // ── 3. Group topics by coPartitionGroup ──────────────────────────────────
    std::map<std::string, std::vector<const TopicSpec*>> namedGroups;
    std::vector<const TopicSpec*> singletons;

    for (const auto& ts : in.topics) {
        if (!ts.coPartitionGroup.empty())
            namedGroups[ts.coPartitionGroup].push_back(&ts);
        else
            singletons.push_back(&ts);
    }

    // ── 4. Result accumulator ───────────────────────────────────────────────
    std::vector<BalancerRecord> result;
    int preferredElectionsUsed = 0;

    // ── 5. Process one group (or singleton wrapped as single-member group) ───
    //
    // For each partition index p:
    //   a. Check each topic member for dead leader → failover.
    //      If ANY member needs failover, all members use the same elected leader
    //      (co-partition group unit move).
    //   b. If no failover needed, check if preferred-leader election would
    //      improve balance: replicas[0] ∈ ISR ∧ live ∧ replicas[0] leads FEWER
    //      partitions than current leader ∧ budget > 0.
    //   c. Apply the change (if any) and skip no-ops.

    auto processGroup = [&](const std::vector<const TopicSpec*>& members)
    {
        if (members.empty()) return;
        const int32_t partCount = members[0]->partitionCount;

        for (int32_t p = 0; p < partCount; ++p) {

            enum class Action { None, Failover, Preferred };
            Action groupAction = Action::None;
            int32_t computedLeader = NO_LEADER_CHANGE;

            const TopicSpec* driver = nullptr;

            for (const TopicSpec* ts : members) {
                const PartitionAssignment* cur = in.cur->get(ts->topicId, p);
                if (!cur) continue;

                // A leader is dead only if it is neither heartbeat-eligible NOR
                // demonstrably alive via the data plane (its partition frontier is
                // advancing — see DynamicBalancerInputs::dataPlaneAliveLeaders).
                // This stops a busy leader whose consensus heartbeat is starved
                // under produce load from being falsely failed over (churn bug),
                // while a genuinely crashed leader (frozen frontier) still fails
                // over.  Time-independent: converges off data-plane progress.
                const bool leaderDead =
                    (cur->leaderNode == NO_LEADER)
                    || (!isIn(cur->leaderNode, in.liveNodes)
                        && !isIn(cur->leaderNode, in.dataPlaneAliveLeaders));

                if (leaderDead) {
                    groupAction = Action::Failover;
                    driver = ts;
                    break;  // failover takes priority
                }

                // Check preferred-leader: replicas[0] ∈ ISR ∧ live ∧ not current leader
                // AND would improve balance (preferred leads fewer than current).
                if (groupAction == Action::None &&
                    preferredElectionsUsed < in.maxPreferredElectionsPerCycle &&
                    !cur->replicas.empty())
                {
                    const int32_t preferred = cur->replicas[0];
                    if (preferred == cur->leaderNode) continue;  // already preferred

                    const bool prefIsLive = isIn(preferred, in.liveNodes);
                    if (!prefIsLive) continue;
                    // Preferred is valid if it is in the ISR, OR — after a transient
                    // liveness collapse dropped it from the ISR — if it is restorable
                    // (caught up per positions / RF==N).  Restoring from outside the
                    // stale ISR is what breaks the "collapsed to survivor forever"
                    // deadlock; the emit path below re-seeds a healthy ISR so the new
                    // leader is in its own ISR and acks=majority can commit again.
                    const bool prefInIsr = isIn(preferred, cur->isr);
                    if (!prefInIsr &&
                        !restorable(ts->topicId, p, preferred, cur->leaderNode)) {
                        continue;
                    }

                    // Only trigger if it improves balance (preferred leads fewer than current).
                    const int32_t prefLoad    = leaderLoad.count(preferred)
                                                ? leaderLoad.at(preferred) : 0;
                    const int32_t currentLoad = leaderLoad.count(cur->leaderNode)
                                                ? leaderLoad.at(cur->leaderNode) : 0;
                    if (prefLoad < currentLoad) {
                        groupAction = Action::Preferred;
                        driver = ts;
                    }
                }
            }

            if (groupAction == Action::None) continue;

            // Determine elected leader.
            if (groupAction == Action::Failover) {
                // ── Safety: elect from the INTERSECTION of ALL member ISRs ∩ liveNodes.
                //
                // Each (topic,partition) replicates independently so member ISRs can
                // diverge over the cluster's lifetime.  A leader elected from only the
                // driver's ISR can be outside a sibling member's ISR → committed loss.
                // We build the group ISR as the set of nodes that are in-sync for ALL
                // members, then run FailoverElection on that intersected ISR.  If the
                // intersection is empty the whole group goes NO_LEADER (safety > avail).
                // ─────────────────────────────────────────────────────────────────────

                // Build groupIsr = ∩(member.isr) ∩ liveNodes, seeded from liveNodes.
                std::vector<int32_t> groupIsr = in.liveNodes;

                for (const TopicSpec* ts : members) {
                    const PartitionAssignment* mc = in.cur->get(ts->topicId, p);
                    if (!mc) { groupIsr.clear(); break; }

                    // Retain in groupIsr only nodes that are also in mc->isr.
                    groupIsr.erase(
                        std::remove_if(groupIsr.begin(), groupIsr.end(),
                            [&mc](int32_t node){ return !isIn(node, mc->isr); }),
                        groupIsr.end());

                    if (groupIsr.empty()) break;
                }

                // NB: NO minIsr floor here.  A single surviving in-ISR replica
                // (0 < |groupIsr| < minIsr) is a VALID failover target: it holds the
                // committed prefix (it was in-sync), so promoting it serves committed
                // data without loss; new acks=all produce correctly blocks at the
                // gateway until the ISR re-expands to minIsr.  Blocking this failover
                // instead stalls the partition on the dead leader FOREVER — clients
                // keep redirecting to the dead endpoint (committed-loss on read).
                //
                // The transient-isolation case a floor once guarded (observer sees a
                // false eligN=1 while peers are alive) is now prevented upstream: the
                // FollowerLiveness state machine keeps every live follower's
                // AppendPosition fresh, so eligibleNodeIds() is ACCURATE — a lone
                // eligible in-ISR member means the others are genuinely gone.  Split
                // brain is still impossible: this runs on the consensus leader, which
                // only holds leadership with a majority quorum.  in.minIsr remains the
                // gateway produce gate (acks=all), not a failover gate.
                if (groupIsr.empty()) {
                    // No node is in-ISR for all members AND live → NO_LEADER for all.
                    computedLeader = NO_LEADER;
                } else {
                    // Run FailoverElection with the intersected ISR to pick a winner.
                    // We synthesise a proxy PartitionAssignment whose ISR is the group
                    // ISR; the driver's replicas/epochs supply the other fields.
                    const PartitionAssignment* driverCur = in.cur->get(driver->topicId, p);
                    if (!driverCur) continue;

                    PartitionAssignment proxyPa   = *driverCur;
                    proxyPa.isr                   = groupIsr;
                    // Force leaderNode to a dead value so FailoverElection always
                    // proceeds (current leader is dead — that's why we're here).
                    proxyPa.leaderNode             = NO_LEADER;

                    ElectionInputs ei;
                    ei.cur       = proxyPa;
                    ei.liveNodes = in.liveNodes;
                    ei.positions = in.positions;

                    ElectionOutcome outcome = FailoverElection::elect(driver->topicId, p, ei);

                    if (outcome.change.newLeader == NO_LEADER_CHANGE) {
                        // Election decided current leader is still fine — shouldn't
                        // happen since we forced leaderNode = NO_LEADER, but guard.
                        continue;
                    }
                    computedLeader = outcome.change.newLeader;
                }

                // Per-member safety assert: computedLeader must be in member.isr ∧ live,
                // OR computedLeader == NO_LEADER (intersection was empty).
                // If intersection logic is correct this always holds; assert defensively.
                if (computedLeader != NO_LEADER) {
                    for (const TopicSpec* ts : members) {
                        const PartitionAssignment* mc = in.cur->get(ts->topicId, p);
                        if (!mc) continue;
                        if (!isIn(computedLeader, mc->isr) || !isIn(computedLeader, in.liveNodes)) {
                            // Should never reach here — intersection guarantees safety.
                            // Defensively fall back to NO_LEADER rather than violate safety.
                            computedLeader = NO_LEADER;
                            break;
                        }
                    }
                }
            } else {
                // Preferred-leader: use replicas[0] of the driver.
                const PartitionAssignment* driverCur = in.cur->get(driver->topicId, p);
                if (!driverCur || driverCur->replicas.empty()) continue;
                const int32_t candidate = driverCur->replicas[0];

                // ── Safety: the candidate must be safe leader for EVERY member.
                //
                // Preferred-leader is a best-effort optimisation, not a safety op.
                // "Safe" = live AND holds the committed prefix for that member —
                // either already in the member's ISR, OR restorable (caught up per
                // positions / RF==N) after a transient-liveness collapse dropped it
                // from the ISR.  Restoration re-seeds the ISR in the emit path below.
                // If it can't move as a unit safely, skip the whole group.
                // ─────────────────────────────────────────────────────────────────────
                bool safeForAll = isIn(candidate, in.liveNodes);
                if (safeForAll) {
                    for (const TopicSpec* ts : members) {
                        const PartitionAssignment* mc = in.cur->get(ts->topicId, p);
                        if (!mc) { safeForAll = false; break; }
                        const bool inIsr = isIn(candidate, mc->isr);
                        if (!inIsr &&
                            !restorable(ts->topicId, p, candidate, mc->leaderNode)) {
                            safeForAll = false;
                            break;
                        }
                    }
                }
                if (!safeForAll) continue;  // can't move group as unit safely → skip

                computedLeader = candidate;
                ++preferredElectionsUsed;
            }

            // Apply `computedLeader` to ALL members in the group for partition p.
            for (const TopicSpec* ts : members) {
                // Enforce batch cap: stop emitting once the limit is reached.
                if (static_cast<int>(result.size()) >= in.maxRecordsPerCycle) return;

                const PartitionAssignment* cur = in.cur->get(ts->topicId, p);
                if (!cur) continue;

                AssignmentChange chg;
                chg.newLeader = computedLeader;

                if (groupAction == Action::Failover) {
                    // New ISR = cur.isr ∩ liveNodes ∩ groupIsr-winner neighbourhood,
                    // with elected leader first.  For the NO_LEADER case the new ISR
                    // is empty (no live in-sync replica for this member).
                    std::vector<int32_t> newIsr;
                    newIsr.reserve(cur->isr.size());
                    if (computedLeader != NO_LEADER) {
                        newIsr.push_back(computedLeader);
                        for (int32_t node : cur->isr) {
                            if (node != computedLeader && isIn(node, in.liveNodes))
                                newIsr.push_back(node);
                        }
                    }
                    chg.newIsr      = std::move(newIsr);
                    chg.newReplicas = {};  // replica set unchanged
                } else {
                    // Preferred-leader.
                    if (isIn(computedLeader, cur->isr)) {
                        // Normal case: elected leader already in ISR → ISR unchanged.
                        chg.newIsr      = {};
                    } else {
                        // Restoration from a collapsed ISR (transient-liveness recovery):
                        // the elected leader was dropped from the ISR during the earlier
                        // failover shrink.  Re-seed a healthy ISR = {newLeader} ∪ the live
                        // restorable replicas (caught up per positions / RF==N), so the
                        // leader is in its own ISR and the set reaches minIsr again.  The
                        // data-plane IsrManager maintains it from here.
                        std::vector<int32_t> newIsr;
                        newIsr.push_back(computedLeader);
                        for (int32_t node : cur->replicas) {
                            if (node == computedLeader) continue;
                            if (isIn(node, in.liveNodes) &&
                                restorable(ts->topicId, p, node, cur->leaderNode))
                                newIsr.push_back(node);
                        }
                        // Retain any current ISR members still live but not already added
                        // (e.g. the survivor that has been leading).
                        for (int32_t node : cur->isr)
                            if (!isIn(node, newIsr) && isIn(node, in.liveNodes))
                                newIsr.push_back(node);
                        chg.newIsr = std::move(newIsr);
                    }
                    chg.newReplicas = {};
                }

                // Merge and detect change.
                PartitionAssignment next = mergeChange(*cur, chg);

                // Skip no-ops: identical assignment → partitionEpoch not bumped.
                if (next.partitionEpoch == cur->partitionEpoch &&
                    next.leaderEpoch    == cur->leaderEpoch    &&
                    next.leaderNode     == cur->leaderNode     &&
                    next.isr            == cur->isr            &&
                    next.replicas       == cur->replicas)
                {
                    continue;
                }

                // Update running load table so subsequent preferred-leader decisions
                // reflect this cycle's changes.
                if (next.leaderNode != cur->leaderNode) {
                    if (cur->leaderNode != NO_LEADER)
                        leaderLoad[cur->leaderNode]--;
                    if (next.leaderNode != NO_LEADER)
                        leaderLoad[next.leaderNode]++;
                }

                // Emit BalancerRecord.
                BalancerRecord br;
                br.isr      = next.isr;
                br.replicas = next.replicas;

                auto& rec         = br.rec;
                rec.topicId        = ts->topicId;
                rec.partition      = p;
                rec.leaderNode     = next.leaderNode;
                rec.leaderEpoch    = next.leaderEpoch;
                rec.partitionEpoch = next.partitionEpoch;
                rec.isr            = makeView(br.isr);
                rec.replicas       = makeView(br.replicas);

                result.push_back(std::move(br));
            }
        }
    };

    // Process named groups (sorted by name for determinism).
    for (const auto& [name, members] : namedGroups)
        processGroup(members);

    // Process singletons in input order.
    for (const TopicSpec* ts : singletons)
        processGroup({ts});

    return result;
}

} // namespace rune::metadata
