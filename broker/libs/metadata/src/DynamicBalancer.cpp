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
        // Internal (coordinator) partitions are assigned by the controller too
        // (re-review MEDIUM-B) but have no gpid (gpidOf requires topicId >= 0)
        // and never count toward user-partition leader balance.
        if (topicId < 0) return;
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

    // (The position-based "restorability" — electing / re-seeding into the ISR a
    //  live replica outside the committed ISR whose reported LEO caught up — is
    //  retired.  Kafka model (user decision 2026-10-03): elections pick ONLY from
    //  the committed ISR; a LEO report proves nothing about lineage (a longer log
    //  may end in an uncommitted old-epoch tail).  A partition leader re-admits
    //  caught-up followers itself (AlterIsr), which is what un-collapses an ISR.)

    // ── 3. Group topics by coPartitionGroup ──────────────────────────────────
    std::map<std::string, std::vector<const TopicSpec*>> namedGroups;
    std::vector<const TopicSpec*> singletons;

    for (const auto& ts : in.topics) {
        if (!ts.coPartitionGroup.empty())
            namedGroups[ts.coPartitionGroup].push_back(&ts);
        else
            singletons.push_back(&ts);
    }

    // ── 3b. Per-node preferred-leader imbalance (N3, Kafka ratio) ──────────
    std::unordered_map<int32_t, int32_t> preferredFor, displacedFrom;
    if (in.imbalancePct > 0) {
        for (const auto& ts : in.topics) {
            for (int32_t p = 0; p < ts.partitionCount; ++p) {
                const PartitionAssignment* cur = in.cur->get(ts.topicId, p);
                if (!cur || cur->replicas.empty()) continue;
                const int32_t pref = cur->replicas[0];
                ++preferredFor[pref];
                if (cur->leaderNode != pref) ++displacedFrom[pref];
            }
        }
    }
    auto imbalanced = [&](int32_t node) {
        if (in.imbalancePct <= 0) return true;
        const int32_t total = preferredFor.count(node) ? preferredFor.at(node) : 0;
        const int32_t off   = displacedFrom.count(node) ? displacedFrom.at(node) : 0;
        return total > 0 && off * 100 > in.imbalancePct * total;
    };

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
            // Set when computedLeader was chosen by the position-proven survivor
            // election (a live replica proven to hold the committed prefix, but not in
            // the stale committed ISR) — the ISR-membership safety assert is satisfied
            // by position proof instead of committed-ISR membership.
            bool positionProven = false;

            const TopicSpec* driver = nullptr;

            // MAJOR-1: leaders excluded at this partition index (a restarted
            // ex-leader that asked for a new epoch) — never re-elected here,
            // never kept in the new ISR.
            std::vector<int32_t> excluded;
            for (const auto& ex : in.excludedLeaders)
                for (const TopicSpec* ts : members)
                    if (ex.topicId == ts->topicId && ex.partition == p && !isIn(ex.node, excluded))
                        excluded.push_back(ex.node);
            auto isExcludedLeader = [&](int32_t topicId, int32_t node) {
                for (const auto& ex : in.excludedLeaders)
                    if (ex.topicId == topicId && ex.partition == p && ex.node == node) return true;
                return false;
            };

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
                    || isExcludedLeader(ts->topicId, cur->leaderNode)
                    || isIn(cur->leaderNode, in.avoidLeaders)   // a stopping leader hands off
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
                    // An excluded node (a restarted ex-leader that asked for a
                    // new epoch, or a node in controlled shutdown) is never a
                    // leadership target — not even the preferred one.
                    if (isExcludedLeader(ts->topicId, preferred)) continue;
                    if (isIn(preferred, in.avoidLeaders)) continue;
                    if (!imbalanced(preferred)) continue;   // N3: below the ratio
                    // Kafka: a preferred election only targets a member of the
                    // committed ISR (the leader's AlterIsr re-admits it first).
                    if (!isIn(preferred, cur->isr)) continue;

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
                if (!excluded.empty()) {
                    const bool hadExcluded = std::any_of(groupIsr.begin(), groupIsr.end(),
                        [&](int32_t n) { return isIn(n, excluded); });
                    std::erase_if(groupIsr, [&](int32_t n) { return isIn(n, excluded); });
                    // The excluded requester is the only live in-sync member:
                    // no clean alternative — the caller re-elects it at a new
                    // epoch (never NO_LEADER / a survivor election from here).
                    if (groupIsr.empty() && hadExcluded) continue;
                }
                // Fix round 2 (N3): a stopping leader still SERVING — heartbeat-
                // live or data-plane alive (its frontier advances) — keeps a
                // partition no non-stopping in-sync replica can take.
                const PartitionAssignment* drv = in.cur->get(driver->topicId, p);
                const bool leaderServingAndStopping =
                    drv && isIn(drv->leaderNode, in.avoidLeaders) &&
                    (isIn(drv->leaderNode, in.liveNodes) || isIn(drv->leaderNode, in.dataPlaneAliveLeaders));
                if (groupIsr.empty() && leaderServingAndStopping) continue;
                if (!in.avoidLeaders.empty() && !groupIsr.empty()) {
                    // Controlled shutdown: prefer a live NON-stopping in-sync
                    // replica.  With none left: a stopping LEADER keeps the
                    // partition (sole in-sync replica — no unclean election);
                    // a DEAD leader's partition goes to the stopping survivor
                    // (better than offline).
                    std::vector<int32_t> keep;
                    for (int32_t n : groupIsr) if (!isIn(n, in.avoidLeaders)) keep.push_back(n);
                    if (!keep.empty()) groupIsr.swap(keep);
                    else if (leaderServingAndStopping) continue;
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
                    // ── Position-proven clean survivor election ────────────────────
                    // The committed ISR (intersected over the group) has NO live
                    // member — it went stale, shrinking to nodes that are now dead
                    // while the live survivors were never re-admitted (data-plane ISR
                    // expand not yet propagated to the committed map).  Going NO_LEADER
                    // here strands the partition forever even though live survivors
                    // hold the committed prefix (dr5_leader_isolation committed-loss).
                    //
                    // SAFETY (no committed loss): a committed record under acks=all was
                    // durable on >= minIsr replicas.  If MORE than (RF - minIsr)
                    // replicas survive, at least one record-holder survives; the
                    // highest-(leaderEpoch,leo) live survivor holds a superset of every
                    // live replica's log, hence the full committed prefix.  Electing it
                    // is a CLEAN election (becomeLeader caps the committed floor at the
                    // new leader's HW view, so no uncommitted tail is exposed).  Gate on
                    // the survivor count so a genuine minority still (correctly) stays
                    // NO_LEADER.  Time-independent: decided off positions, not timing.
                    computedLeader = NO_LEADER;
                    positionProven = false;
                    // MEDIUM-3: opt-in only (Kafka: offline unless unclean
                    // election is enabled) — default stays NO_LEADER below.
                    for (const TopicSpec* ts : members) {
                        if (!in.allowSurvivorElection) break;
                        const PartitionAssignment* mc = in.cur->get(ts->topicId, p);
                        if (!mc || mc->replicas.empty() || !in.positions) {
                            computedLeader = NO_LEADER; positionProven = false; break;
                        }
                        const int rf = static_cast<int>(mc->replicas.size());
                        // Count only live replicas that REPORTED a position: an
                        // unreported one may be the only live holder, and the best
                        // reported replica would then lack committed records.
                        int liveInReplicas = 0;
                        for (int32_t r : mc->replicas)
                            if (isIn(r, in.liveNodes) && in.positions->get(ts->topicId, p, r).has_value())
                                ++liveInReplicas;
                        // Need strictly more than (RF - minIsr) survivors for the
                        // pigeonhole completeness guarantee to hold.
                        if (liveInReplicas < (rf - in.minIsr + 1)) {
                            computedLeader = NO_LEADER; positionProven = false; break;
                        }
                        // Highest-(epoch,leo) live replica WITH a position report.
                        int32_t best = NO_LEADER;
                        rune::consensus::Term bestEp = -1; int64_t bestLeo = -1;
                        for (int32_t r : mc->replicas) {
                            if (!isIn(r, in.liveNodes)) continue;
                            auto opt = in.positions->get(ts->topicId, p, r);
                            if (!opt.has_value()) continue; // no proof → not a candidate
                            const auto ep  = static_cast<rune::consensus::Term>(opt->leaderEpoch);
                            const auto leo = opt->logEndPosition;
                            const int cmp = (best == NO_LEADER) ? 1
                                : rune::consensus::ClusterMember::compareLog(ep, leo, bestEp, bestLeo);
                            if (cmp > 0 || (cmp == 0 && r < best)) {
                                best = r; bestEp = ep; bestLeo = leo;
                            }
                        }
                        if (best == NO_LEADER) { computedLeader = NO_LEADER; positionProven = false; break; }
                        // All members of a co-partition group must agree on one leader:
                        // require the SAME survivor across members (they co-partition).
                        if (computedLeader == NO_LEADER) computedLeader = best;
                        else if (computedLeader != best) { computedLeader = NO_LEADER; positionProven = false; break; }
                        positionProven = true;
                    }
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
                // EXCEPTION: a position-proven survivor election (positionProven) elects
                // a live replica that is NOT in the stale committed ISR but is proven by
                // the ReplicaPositionTable to hold the committed prefix — its safety comes
                // from position proof + the survivor-count gate, not ISR membership, so we
                // only require it to be live (never let it be outside the replica set,
                // which the election above already guarantees).
                if (computedLeader != NO_LEADER && !positionProven) {
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
                } else if (computedLeader != NO_LEADER && positionProven) {
                    // Position-proven: only require liveness (defence-in-depth).
                    if (!isIn(computedLeader, in.liveNodes)) computedLeader = NO_LEADER;
                }
            } else {
                // Preferred-leader: use replicas[0] of the driver.
                const PartitionAssignment* driverCur = in.cur->get(driver->topicId, p);
                if (!driverCur || driverCur->replicas.empty()) continue;
                const int32_t candidate = driverCur->replicas[0];

                // ── Safety: the candidate must be safe leader for EVERY member.
                //
                // Preferred-leader is a best-effort optimisation, not a safety op.
                // "Safe" = live AND in the member's committed ISR (Kafka).  If it
                // can't move as a unit safely, skip the whole group.
                // ─────────────────────────────────────────────────────────────────────
                bool safeForAll = isIn(candidate, in.liveNodes);
                if (safeForAll) {
                    for (const TopicSpec* ts : members) {
                        const PartitionAssignment* mc = in.cur->get(ts->topicId, p);
                        if (!mc) { safeForAll = false; break; }
                        if (!isIn(candidate, mc->isr)) {
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
                    if (computedLeader != NO_LEADER) {
                        // New ISR = cur.isr ∩ liveNodes with the elected leader first.
                        std::vector<int32_t> newIsr;
                        newIsr.reserve(cur->isr.size());
                        newIsr.push_back(computedLeader);
                        for (int32_t node : cur->isr) {
                            if (node != computedLeader && isIn(node, in.liveNodes) &&
                                !isIn(node, excluded) && !isIn(node, in.avoidLeaders))
                                newIsr.push_back(node);
                        }
                        chg.newIsr = std::move(newIsr);
                    } else {
                        // NO_LEADER (no live in-sync survivor): RETAIN the ISR lineage
                        // — do NOT clear it.  cur.isr is the set of replicas that held
                        // the committed prefix when the last survivor died.  Clearing
                        // it to {} made the partition PERMANENTLY offline: every later
                        // tick recomputed proposedIsr = {} ∩ liveNodes = {} → NO_LEADER
                        // forever, even after a former-ISR replica rejoined caught-up
                        // (dr3 rolling leader-kill: partitions stuck NO_LEADER, produce
                        // 44/50).  Retaining cur.isr lets a returning in-sync replica be
                        // cleanly re-elected on a later tick (FailoverElection then finds
                        // a non-empty proposed ISR).  Kafka parity: an offline partition
                        // keeps its ISR lineage; recovery is a clean election, never
                        // unclean.  newIsr == cur.isr → mergeChange applies only the
                        // leader→NO_LEADER flip, no spurious ISR churn.
                        chg.newIsr = cur->isr;
                    }
                    chg.newReplicas = {};  // replica set unchanged
                } else {
                    // Preferred-leader: the elected leader is in every member's
                    // committed ISR (checked above) → the ISR is carried over
                    // unchanged (leader first).  NB mergeChange applies newIsr
                    // literally — an empty one would EMPTY the committed ISR.
                    std::vector<int32_t> newIsr;
                    newIsr.reserve(cur->isr.size());
                    newIsr.push_back(computedLeader);
                    for (int32_t node : cur->isr)
                        if (node != computedLeader) newIsr.push_back(node);
                    chg.newIsr = std::move(newIsr);
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
