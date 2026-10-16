#pragma once
// ---------------------------------------------------------------------------
// MetadataService.h — MD1 Task 4 + Task 6: thin driver that owns the MSM
// (rune::metadata::MetadataStateMachine) and applies committed metadata-log
// records on the duty cycle.
//
// Design (mirrors GroupCoordinationService / TransactionCoordinationService):
//   - owns a MetadataStateMachine (the committed partition-assignment map)
//   - on construction: caller supplies the metadata PartitionReplica* (topicId=-3)
//   - doWork(nowNs): reads the replica's highWatermark() and calls
//       msm_.commit(hw) so all pending records with pos <= hw become visible.
//     Also replays any new batches from the committed log tail on the
//     leader-transition edge (wasLeader_ gate, mirrors TCS::doWork).
//   - rebuildFromLog(): replays the entire committed log [0, HW) and rebuilds
//     the MSM from scratch — called on restart or leader failover
//     (mirrors ReplicatedTxnStateStore::rebuildFromLog).
//   - assignment(topicId, partition): delegates to msm_.assignment().
//
// HW-gated apply:
//   The MSM uses a pendingPos buffer.  A record is NOT visible via assignment()
//   until commit(hw) is called with hw >= record's log position.  doWork()
//   calls commit(replica->highWatermark()) each cycle — so assignment() reflects
//   only COMMITTED records (the HW-gate invariant).
//
// Single-writer: all methods must be called on the duty-cycle thread.
//   No locks.  No hot-path allocation beyond the first doWork after a burst.
//
// CONTROLLER RULING (FINDING-1):
//   The __cluster_metadata partition's OWN leadership follows cluster-consensus
//   leadership, NOT the assignment map.  The existing ReplicationMetadataBridge
//   broadcasts cluster leadership to ALL registered replicas, so simply
//   registering metadataReplica_ in ReplicaRegistry makes the bridge drive its
//   leader/follower role automatically.  MetadataService does NOT attempt to
//   drive its own replica's role.
//
// MD1 Task 6 — Controller emits static assignment on leader claim:
//   - configure(topics, liveNodes): supplies the topic + node context for
//     StaticBalancer::assign().  Must be called before start().
//   - emitStaticAssignmentIfNeeded(): called by doWork() on the Follower→Leader
//     edge.  Runs StaticBalancer::assign() and appends one record per
//     replica_->append() call — each metadata record occupies its own log slot.
//     This is the ONLY correct design: concatenating N encoded records into one
//     payload would cause only the first record to be decoded on replay.
//     Idempotency: if the MSM already has assignments for ALL user topics in
//     topics_, emitting is skipped.  The "assigned_" flag latches after the
//     first successful emit so re-observing leadership is also a no-op.
//   - balancerErrorCount(): error counter; incremented when BalancerError is
//     caught (misconfigured group, RF > nodes, etc.).  Non-zero = silent failure;
//     use this to detect bad config in tests or monitoring.
//
// USER topics (topicId >= 0) are assigned by the balancers.  Multi-node, the
// COORDINATOR partitions (__consumer_offsets=-1, __transaction_state=-2) and
// __catalog (-6, catalog history Task 6) are
// assigned by this controller too (re-review MEDIUM-B, see
// decideCoordinatorPartition).  __cluster_metadata (-3) follows cluster
// leadership and is NOT assigned.
//
// Namespace: rune::node
// C++23. Single-writer. No locks.
// ---------------------------------------------------------------------------

#include <rune/metadata/MetadataStateMachine.h>
#include <rune/metadata/PartitionAssignment.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/StaticBalancer.h>
#include <rune/metadata/DynamicBalancer.h>       // Task 14: rebalance-on-tick
#include <rune/metadata/FailoverElection.h>      // MEDIUM-B: coordinator partitions
#include <rune/metadata/ControlPlaneTimer.h>     // Task 14: deterministic logged tick
#include <rune/metadata/ReplicaPositionTable.h>  // Task 14: election position input
#include <rune/metadata/MetadataExtensions.h>    // D15: feature level + leaderless marks; C1 lineages
#include <chrono>
#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/RecordBatch.h>  // RecordBatchReader
#include <rune/protocol/MetadataRecords.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>   // std::abort (LOW-1 fail-stop)
#include <deque>
#include <utility>
#include <optional>
#include <map>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace rune::node {

// ---------------------------------------------------------------------------
// MetadataService
// ---------------------------------------------------------------------------
class MetadataService {
public:
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// @param replica  Non-owning pointer to the __cluster_metadata
    ///                 PartitionReplica (topicId=-3, partitionId=0).
    ///                 Must outlive this object.
    /// @param selfId   This node's NodeId (for logging / delta derivation).
    MetadataService(rune::replication::PartitionReplica* replica,
                    int32_t selfId)
        : replica_(replica)
        , selfId_(selfId)
    {}

    MetadataService(const MetadataService&)            = delete;
    MetadataService& operator=(const MetadataService&) = delete;

    // -----------------------------------------------------------------------
    // MD1 Task 6: configure topic + node context for static assignment emit.
    // -----------------------------------------------------------------------

    /// Supply the topic + live-node context used by emitStaticAssignmentIfNeeded().
    ///
    /// @param topics      User topic specs (topicId >= 0; internal topics are
    ///                    filtered out automatically).
    /// @param liveNodes   Live broker NodeIds (sorted ascending internally by
    ///                    StaticBalancer::assign).
    ///
    /// Must be called on the duty-cycle thread before start().
    void configure(const std::vector<rune::metadata::TopicSpec>& topics,
                   const std::vector<rune::metadata::NodeId>&    liveNodes,
                   int32_t /*batchSize_unused*/ = 256)
    {
        // Filter: only user topics (topicId >= 0).
        liveNodes_ = liveNodes;
        configuredTopics_.clear();
        configuredTopics_.reserve(topics.size());
        for (const auto& ts : topics) {
            if (ts.topicId >= 0) {
                configuredTopics_.push_back(ts);
                if (configuredTopics_.back().rf <= 0) resolveDefaultRf_(configuredTopics_.back());   // M3
            }
        }
        assigned_  = false;
        ++routingInputsEpoch_;
    }

    /// Set the commit-quorum failover floor handed to the DynamicBalancer.  A
    /// failover never installs an ISR below this (prevents the ISR<minIsr startup
    /// collapse under transient heartbeat isolation).  BrokerNode passes
    /// gatewayMinIsr (cluster majority).
    void setMinIsr(int minIsr) noexcept { minIsr_ = minIsr > 0 ? minIsr : 1; }
    /// MEDIUM-3: opt in to the position-proven survivor election (default OFF).
    void setAllowSurvivorElection(bool on) noexcept { allowSurvivorElection_ = on; }

    // -----------------------------------------------------------------------
    // D15 — metadata feature level (KRaft metadata.version).  The controller
    // commits a FeatureLevelRecord raising the cluster level to L only once
    // EVERY cluster member advertised support for L (its node-control
    // requests carry AlterIsrMsg::kNodeFeatureLevelSlot) — or, on a brand-new
    // metadata log (nothing to be compatible with), at its bootstrap.  Request
    // kinds an older controller would silently drop are sent only once the
    // COMMITTED level supports them (ReplicationMetadataBridge stamps it on
    // every role op; PartitionReplica::setMetadataFeatureLevel).
    // -----------------------------------------------------------------------
    /// Every cluster member (incl. self).  Without it the level is never
    /// raised past the bootstrap (membership unknown).
    void setClusterMembers(std::vector<int32_t> members) { clusterMembers_ = std::move(members); }
    /// The level THIS binary supports (tests emulate an older one).
    void setSupportedFeatureLevel(int16_t l) noexcept { supportedFeatureLevel_ = l; }
    /// The committed cluster feature level.
    [[nodiscard]] int16_t featureLevel() const noexcept { return msm_.featureLevel(); }

    // -----------------------------------------------------------------------
    // Re-review 2026-10-04 MEDIUM-B — the COORDINATOR partitions
    // (__consumer_offsets -1, __transaction_state -2) on the controller-ISR
    // (user-partition) path.  Only __cluster_metadata decides consensus
    // candidacy (InternalIsrKnowledge); these partitions are assigned by this
    // controller like user partitions: their ISR is committed here (AlterIsr,
    // partitionEpoch CAS, KIP-841 fencing), their leader is elected from it,
    // and with no live ISR member they are OFFLINE (NO_LEADER, lineage kept) —
    // one of their ISRs can no longer take the whole control plane down.
    //
    // Coordinator colocation: the group / transaction coordinators run on the
    // consensus leader (this controller) and load only while its replica of
    // the partition leads.  So the controller PREFERS ITSELF — but only when it
    // is in the committed ISR (a clean move).  Otherwise a live ISR member
    // leads; this node fetches from it, that leader's AlterIsr admits it once
    // caught up, and the next tick moves leadership here.  Multi-node only
    // (BrokerNode); single-node keeps the consensus-driven path.
    // -----------------------------------------------------------------------
    //
    // Catalog history Task 6 (ruling 7): __catalog (-6) is on this path too —
    // replicas = every broker, leader elected only from ISR ∩ live
    // (allowUnclean = false), the controller preferring itself when in the ISR.
    [[nodiscard]] static constexpr bool isControllerIsrInternalTopic(int32_t topicId) noexcept {
        return topicId == -1 || topicId == -2 || topicId == -6;
    }
    /// Enable the controller path for `keys` (replicated on `replicas`).
    void setCoordinatorPartitions(std::vector<rune::metadata::PartitionKey> keys,
                                  std::vector<int32_t> replicas) {
        coordinatorKeys_     = std::move(keys);
        coordinatorReplicas_ = std::move(replicas);
    }
    [[nodiscard]] bool coordinatorKey_(int32_t topicId, int32_t partition) const noexcept {
        for (const auto& k : coordinatorKeys_)
            if (k.topicId == topicId && k.partition == partition) return true;
        return false;
    }

    /// The controller's decision for one coordinator partition (pure; cold).
    /// Returns the next assignment to commit, nullopt = nothing to change.
    ///   - none yet (fresh cluster): self leads at `bootstrapEpoch`, every
    ///     replica seeds the ISR (as a new user partition);
    ///   - self leads: nothing (fencing / AlterIsr maintain the ISR);
    ///   - self in the committed ISR: move here (clean, leaderEpoch + 1);
    ///   - a live leader in its ISR: keep it (self catches up first);
    ///   - else FailoverElection over ISR ∩ live (never unclean): a live ISR
    ///     member, or NO_LEADER keeping the ISR lineage.
    static std::optional<rune::metadata::PartitionAssignment> decideCoordinatorPartition(
        int32_t topicId, int32_t partition, int32_t selfId,
        const rune::metadata::PartitionAssignment* cur,
        const std::vector<int32_t>& eligible,
        rune::metadata::ReplicaPositionTable* positions,
        int32_t bootstrapEpoch,
        const std::vector<int32_t>& replicas,
        bool selfShutting = false,
        const std::vector<int32_t>* avoid = nullptr,
        const std::vector<int32_t>* shutting = nullptr)
    {
        namespace rm = rune::metadata;
        auto in = [](int32_t n, const std::vector<int32_t>& v) {
            return std::find(v.begin(), v.end(), n) != v.end();
        };
        // Task 11 fix round 1 (M2): `avoid` (the catalog-halted brokers, for
        // __catalog only) are never preferred while a live, non-avoided ISR member
        // exists — a halted catalog leader would answer every DDL Internal.  With
        // none, the rules below apply unchanged (a leader beats no leader).
        auto avoided = [&](int32_t n) { return avoid != nullptr && in(n, *avoid); };
        // F-A I1: a member this controller knows is stopping (controlled shutdown) never stays in the ISR of a record
        // that MOVES or FAILS OVER a coordinator partition.  Under the leader grace `live` holds every peer, so without
        // this a cleanly stopped broker kept the __catalog ISR (HW = min over the ISR) across every consensus flip.
        auto stopping = [&](int32_t n) { return shutting != nullptr && in(n, *shutting); };
        if (cur == nullptr) {
            // Bootstrap seeds the ISR with EVERY configured replica, including a
            // broker that never started (catalog history Task 6, review M6):
            // HW = min over the ISR, so such a partition (e.g. __catalog formed
            // without one of its brokers) commits nothing until the fencing pass
            // / the leader's lag shrink drops that broker — a bounded formation
            // stall (≈ the fencing timeout, at most replicaLagMaxMs), never a
            // commit without the majority.  Pinned by CatalogReplicationRegression.
            // ANewBrokerWithAnEmptyDataDirFetchesTheCatalogFromOffsetZero.
            rm::PartitionAssignment pa;
            pa.leaderNode     = selfId;
            pa.isr            = replicas;
            pa.replicas       = replicas;
            pa.leaderEpoch    = bootstrapEpoch;
            pa.partitionEpoch = 0;
            auto it = std::find(pa.isr.begin(), pa.isr.end(), selfId);
            if (it != pa.isr.end()) std::rotate(pa.isr.begin(), it, it + 1);   // leader first
            return pa;
        }
        std::vector<int32_t> live = eligible;
        if (!in(selfId, live)) live.push_back(selfId);   // the controller is live
        bool healthyIsr = false;   // a live, non-avoided ISR member exists
        for (int32_t n : cur->isr) healthyIsr = healthyIsr || (in(n, live) && !avoided(n));
        const bool steer = healthyIsr && avoid != nullptr && !avoid->empty();
        if (cur->leaderNode == selfId && !(steer && avoided(selfId))) return std::nullopt;
        // The leadership CANDIDATES (live, minus the avoided when steering); the
        // ISR keeps every live member (a halted broker still replicates).
        std::vector<int32_t> candidates = live;
        if (steer) std::erase_if(candidates, [&](int32_t n) { return avoided(n); });
        // I1: a stopping member is never ELECTED either while a live, non-stopping candidate in the ISR exists
        // (otherwise it re-enters the ISR as the leader).  With none, the rules below apply unchanged (Kafka: a
        // partition whose only in-sync replica is stopping stays with it, never unclean).
        bool cleanNonStopping = false;
        for (int32_t n : cur->isr) cleanNonStopping = cleanNonStopping || (in(n, candidates) && !stopping(n));
        if (cleanNonStopping) std::erase_if(candidates, [&](int32_t n) { return stopping(n); });
        rm::AssignmentChange chg;
        if (in(selfId, cur->isr) && !selfShutting && in(selfId, candidates)) {   // a stopping controller never takes one
            chg.newLeader = selfId;
            chg.newIsr.push_back(selfId);
            for (int32_t n : cur->isr) if (n != selfId && in(n, live) && !stopping(n)) chg.newIsr.push_back(n);
        } else if (cur->leaderNode != rm::NO_LEADER && in(cur->leaderNode, candidates) &&
                   in(cur->leaderNode, cur->isr)) {
            return std::nullopt;   // (a halted leader is not a candidate when steering: replaced)
        } else {
            rm::ElectionInputs ei;
            ei.cur            = *cur;
            ei.cur.leaderNode = rm::NO_LEADER;   // dead (or none): always elect
            ei.liveNodes      = candidates;
            ei.positions      = positions;
            ei.allowUnclean   = false;
            const rm::ElectionOutcome out = rm::FailoverElection::elect(topicId, partition, ei);
            if (out.change.newLeader == rm::NO_LEADER_CHANGE) return std::nullopt;
            chg.newLeader = out.change.newLeader;
            if (chg.newLeader != rm::NO_LEADER) {
                chg.newIsr.push_back(chg.newLeader);
                for (int32_t n : cur->isr)
                    if (n != chg.newLeader && in(n, live) && !stopping(n)) chg.newIsr.push_back(n);
            } else {
                chg.newIsr = cur->isr;   // offline: keep the lineage (Kafka)
            }
        }
        rm::PartitionAssignment next = rm::mergeChange(*cur, chg);
        if (next.partitionEpoch == cur->partitionEpoch && next.leaderNode == cur->leaderNode &&
            next.isr == cur->isr)
            return std::nullopt;   // no-op
        return next;
    }

    /// Leader-only: apply decideCoordinatorPartition to every coordinator
    /// partition with no decision in flight; enqueues the records.  Called from
    /// runDynamicRebalanceOnTick (cold).  Returns the records enqueued.
    std::size_t reconcileCoordinatorPartitions(const std::vector<int32_t>& eligibleNodes) {
        if (!controllerReady_() || coordinatorKeys_.empty()) return 0;
        std::size_t n = 0;
        const int32_t bootstrapEpoch =
            std::max<int32_t>(1, static_cast<int32_t>(replica_->epoch()));   // the consensus term
        for (const auto& k : coordinatorKeys_) {
            if (inflight_.count(partitionKey_(k.topicId, k.partition)) != 0) continue;
            bool queued = false;
            for (const auto& q : pendingDynamicRecords_)
                queued = queued || (q.rec.topicId == k.topicId && q.rec.partition == k.partition);
            if (queued) continue;
            if (k.topicId == -6) {   // __catalog: steer the leadership off catalog-halted brokers (M2)
                haltedScratch_.clear();
                for (const auto& [n, st] : nodeLifecycle_) if (st.catalogHalted) haltedScratch_.push_back(n);
            }
            const auto next = decideCoordinatorPartition(
                k.topicId, k.partition, selfId_, msm_.assignment(k.topicId, k.partition),
                eligibleNodes, &positions_, bootstrapEpoch, coordinatorReplicas_, isShutting_(selfId_),
                k.topicId == -6 ? &haltedScratch_ : nullptr, &shuttingNodes_());   // I1: every coordinator partition
            if (!next) continue;
            rune::metadata::BalancerRecord br;
            br.isr      = next->isr;
            br.replicas = next->replicas;
            br.rec.topicId        = k.topicId;
            br.rec.partition      = k.partition;
            br.rec.leaderNode     = next->leaderNode;
            br.rec.leaderEpoch    = next->leaderEpoch;
            br.rec.partitionEpoch = next->partitionEpoch;
            br.rec.isr      = rune::protocol::metadata::Int32ArrayView{br.isr.data(), static_cast<uint16_t>(br.isr.size())};
            br.rec.replicas = rune::protocol::metadata::Int32ArrayView{br.replicas.data(), static_cast<uint16_t>(br.replicas.size())};
            pendingDynamicRecords_.push_back(std::move(br));
            ++n;
        }
        if (n > 0) drainDynamicQueue();
        return n;
    }

    // -----------------------------------------------------------------------
    // Task 8b (RULING-4, SINGLE-NODE): register a RUNTIME-defined topic and
    // emit its initial static assignment.
    //
    // Called by the CONTROL thread's create-topic hook AFTER
    // TopicRegistry::createTopic() returns an authoritative id and the user
    // PartitionReplica(s) have been created + registered.  It mirrors the boot
    // assignment path (emitStaticAssignmentIfNeeded), scoped to the ONE new
    // topic:
    //   1. Appends `spec` to configuredTopics_ so the runtime topic participates
    //      in the RoutingImage build (buildStaticRoutingImage) AND future
    //      dynamic rebalances, and so msmAlreadyHasAllAssignments() accounts for
    //      it after a restart-rebuild.
    //   2. If this node is the metadata Leader, runs StaticBalancer::assign()
    //      for JUST this topic and appends one record per (topic, partition) to
    //      the replicated __cluster_metadata log (one-record-per-append).  The
    //      committed records flow through doWork()→MSM→committedMap(), which the
    //      ReplicationMetadataBridge then uses (applyUserPartitionRoles) to
    //      promote the runtime replica to Leader — exactly like a boot topic.
    //
    // Does NOT touch the assigned_ latch (that guards the boot one-shot emit).
    // Idempotent per topic: if the MSM already has an assignment for every
    // partition of this topic (restart / re-invocation), emission is skipped.
    //
    // No-op emission (still records the topic in configuredTopics_) if:
    //   - This replica is not the Leader (single-node this node is the leader).
    //   - liveNodes_ is empty (not yet configured).
    //   - StaticBalancer throws (misconfigured — should not occur single-node).
    //
    // Single-writer: must be called on the duty-cycle / control thread.
    void addRuntimeTopic(rune::metadata::TopicSpec spec) {
        if (spec.topicId < 0) return;  // internal topics are not assigned
        // rf 0 = every PLACEABLE broker (Task 11 fix round 0: catalog-halted
        // brokers are never placement targets).
        if (spec.rf <= 0) resolveDefaultRf_(spec);

        // (1) Record the topic in the configured set (idempotent by topicId) so
        // it participates in routing-image builds + future rebalances.
        bool present = false;
        for (const auto& ts : configuredTopics_) {
            if (ts.topicId == spec.topicId) { present = true; break; }
        }
        if (!present) { configuredTopics_.push_back(spec); ++routingInputsEpoch_; }
        if (placementHeldForTest_) [[unlikely]] return;   // Task 22 seam: recorded, not placed (holdPlacementForTest)

        // (2) Emit the initial assignment for this topic (leader only).
        if (!controllerReady_()) return;
        if (liveNodes_.empty()) return;
        // Task 11 fix round 0: a halted controller places no catalog topic (the
        // applied catalog it would place from is stale).
        if (catalogHalted(selfId_)) { ++catalogPlacementsSkippedHalted_; return; }

        // Per-topic idempotency: skip if the MSM already has every partition
        // assigned (e.g. re-invocation after a restart-rebuild replayed it).
        bool allAssigned = spec.partitionCount > 0;
        for (int32_t p = 0; p < spec.partitionCount; ++p) {
            if (msm_.assignment(spec.topicId, p) == nullptr) { allAssigned = false; break; }
        }
        if (allAssigned) return;

        std::vector<rune::metadata::TopicSpec> one{spec};
        (void)placeUnit_(one);   // an unplaceable topic is held explicitly (unplaceable_), retried
        drainDynamicQueue();
    }

    // -----------------------------------------------------------------------
    // Catalog history Task 11 fix round 1 (I1/I2): placement is PER UNIT (a
    // topic, or a co-partition group) — one unplaceable topic never blocks
    // another.  A unit the balancer refuses (e.g. rf above the placeable
    // brokers, a catalog-halted broker never being a target) enters the
    // explicit Unplaceable state: logged once (and on a reason change), counted
    // (unplaceableTopicCount(), the rune-catalog-unplaceable-topics gauge), and
    // retried whenever the placement inputs change (routingInputsEpoch_: a halt
    // mark set or cleared, a configure) and on every controller catch-up.
    // -----------------------------------------------------------------------
    struct UnplaceableTopic { std::string reason; };
    /// Task 11 fix round 4: one live "cannot serve" report (refreshed by its sender).
    struct UnservableReport { int32_t topicId; int32_t partition; int32_t node; int64_t lastSeenNs; };
    /// Never-placed TOPICS (the rune-catalog-unplaceable-topics gauge).  BLOCKED
    /// PARTITIONS have their own gauge (review M5: never summed together).
    [[nodiscard]] std::size_t unplaceableTopicCount() const noexcept { return unplaceable_.size(); }
    /// I1 (round 5): a controller whose PROCESS has held the broadcast reports for
    /// less than `ns` defers user-partition leader elections until it has (it may
    /// lack a fresh "cannot serve" report).  BrokerNode sets kUnservableReportTtlNs
    /// multi-node; 0 (unit rigs) = off.
    void setUnservableReportWarmup(int64_t ns) noexcept { unservableWarmupNs_ = ns; }
    [[nodiscard]] bool unservableWarmingUp() const noexcept {
        return unservableWarmupNs_ > 0 && (firstPassNs_ < 0 || lastNowNs_ - firstPassNs_ < unservableWarmupNs_);
    }

    // -----------------------------------------------------------------------
    // Catalog history Task 11 fix round 4 — replicas that cannot SERVE.
    //
    // A broker in a partition's replicas that has not built it (its catalog
    // never applied the create, or its apply halted) sends a LIVE, periodic
    // report (kBumpReplicaUnservable).  The controller keeps only fresh reports
    // (expired after kUnservableReportTtlNs without a refresh), and derives ONE
    // thing from them: the node is excluded from that partition's leader
    // ELECTION (DynamicBalancerInputs::electionExcluded).  Nothing else — no
    // replica-set or ISR edit, no metadata record; a replica that cannot fetch
    // leaves the ISR only by the existing lag rules.  When no live in-sync
    // member remains electable the partition is BLOCKED: explicit, counted,
    // logged — never led by an empty replica; it recovers when the holder
    // serves again (its reports stop).  A new controller rebuilds the set from
    // the next reports (nothing goes stale).
    // -----------------------------------------------------------------------
    static constexpr int64_t kUnservableReportTtlNs = 2'000'000'000LL;   // 4 report periods
    [[nodiscard]] bool reportedUnservable(int32_t topicId, int32_t partition, int32_t node) const noexcept {
        for (const auto& u : unservable_)
            if (u.topicId == topicId && u.partition == partition && u.node == node) return true;
        return false;
    }
    [[nodiscard]] std::size_t unservableReportCount() const noexcept { return unservable_.size(); }
    [[nodiscard]] std::size_t blockedPartitionCount() const noexcept { return blocked_.size(); }
    [[nodiscard]] bool blocked(int32_t topicId, int32_t partition) const noexcept {
        return std::find(blocked_.begin(), blocked_.end(), partitionKey_(topicId, partition)) != blocked_.end();
    }
    /// The reason `topicId` is unplaceable ("" = it is not).
    [[nodiscard]] std::string unplaceableReason(int32_t topicId) const {
        const auto it = unplaceable_.find(topicId);
        return it == unplaceable_.end() ? std::string{} : it->second.reason;
    }
    [[nodiscard]] uint64_t unplaceableEvents() const noexcept { return unplaceableEvents_; }

    // -----------------------------------------------------------------------
    // Catalog history Task 8: the catalog DROPPED topic `topicId`.  Removes it
    // from configuredTopics_ (no routing-image build, rebalance or balancer
    // decision names it again), purges any of its records decided but not yet
    // appended, and remembers it in removedTopics_.  The controller (when ready — now, or on its catch-up
    // edge, or as soon as a successor controller is ready) appends ONE
    // RemoveTopicPlacementRecord; at commit every replica of it sees a delete.
    // Never stranded: a broker that is not the controller still records it, so
    // whichever broker becomes controller next completes the removal.
    // Idempotent (a second call changes nothing and appends nothing more).
    // Single-writer: control thread.
    // -----------------------------------------------------------------------
    /// Task 11 fix round 0: `node`'s current process reported a halted applier.
    [[nodiscard]] bool catalogHalted(int32_t node) const noexcept {
        const auto it = nodeLifecycle_.find(node);
        return it != nodeLifecycle_.end() && it->second.catalogHalted;
    }
    /// The nodes a NEW partition may be placed on: the live nodes minus every
    /// catalog-halted one (it never applies the topic — it would never serve it).
    [[nodiscard]] std::vector<int32_t> placementNodes() const {
        std::vector<int32_t> out;
        out.reserve(liveNodes_.size());
        for (int32_t n : liveNodes_) if (!catalogHalted(n)) out.push_back(n);
        return out;
    }
    /// A live (in `live`), not stopping, not catalog-halted member other than self:
    /// a halted controller hands the controllership to it.
    [[nodiscard]] bool healthyPeerAmong(std::span<const int32_t> live) const noexcept {
        for (int32_t n : live)
            if (n != selfId_ && !catalogHalted(n) && !isShutting_(n)) return true;
        return false;
    }
    [[nodiscard]] uint64_t catalogPlacementsSkippedHalted() const noexcept { return catalogPlacementsSkippedHalted_; }

    void removeRuntimeTopic(int32_t topicId) {
        if (topicId < 0) return;   // internal partitions are never removed this way
        const auto before = configuredTopics_.size();
        std::erase_if(configuredTopics_, [topicId](const rune::metadata::TopicSpec& ts) { return ts.topicId == topicId; });
        if (configuredTopics_.size() != before) ++routingInputsEpoch_;
        std::erase_if(pendingDynamicRecords_,
                      [topicId](const rune::metadata::BalancerRecord& br) { return br.rec.topicId == topicId; });
        std::erase_if(pendingLineageRecords_,
                      [topicId](const rune::metadata::TransientLineageRecord& r) { return r.topicId == topicId; });
        const auto at = std::lower_bound(removedTopics_.begin(), removedTopics_.end(), topicId);
        if (at == removedTopics_.end() || *at != topicId) removedTopics_.insert(at, topicId);   // sorted
        reconcileRemovedTopics_(/*force=*/true);
    }
    /// Test-only: every topic id handed to removeRuntimeTopic in this process (sorted) — a restart's
    /// boot hands it every id the catalog dropped (Task 16, B3).
    [[nodiscard]] const std::vector<int32_t>& removedTopicIdsForTest() const noexcept { return removedTopics_; }
    /// Test-only: placement removals decided but not yet committed (queued + in flight).
    [[nodiscard]] std::size_t removalsInFlightForTest() const noexcept {
        return pendingRemovalRecords_.size() + removalInflight_.size();
    }

    /// Emit static assignment records to the metadata log if needed.
    ///
    /// This is the leader-claim action.  It:
    ///   1. Checks the idempotency guard (assigned_ flag + MSM already-has-all).
    ///   2. Runs StaticBalancer::assign(configuredTopics_, liveNodes_).
    ///   3. Appends one record per replica_->append() call — each metadata record
    ///      occupies its own log slot (the ONLY correct design; see comment below).
    ///
    /// No-op if:
    ///   - This replica is not the Leader.
    ///   - configuredTopics_ is empty or liveNodes_ is empty.
    ///   - The MSM already has committed assignments for all user topics.
    ///   - assigned_ is true (latched after first successful emit).
    ///
    /// Public for test-only direct invocation; normally called by doWork().
    /// Single-writer: must be called on the duty-cycle thread.
    void emitStaticAssignmentIfNeeded() {
        if (!controllerReady_()) return;
        // Task 16 fix round 1 (R3): a halted controller places nothing from its stale catalog (Task 11 ruling) —
        // the boot now feeds every catalog topic through configure(), so this path must hold too.  Not latched.
        if (catalogHalted(selfId_)) { ++catalogPlacementsSkippedHalted_; return; }
        if (configuredTopics_.empty() || liveNodes_.empty()) return;

        // Idempotency guard: if already assigned (flag latched), skip.
        if (assigned_) return;

        // Secondary idempotency check: if MSM already has ALL user-topic
        // partition assignments committed, skip re-emission.
        if (msmAlreadyHasAllAssignments()) {
            assigned_ = true;
            return;
        }

        // Task 11 fix round 1 (I1): per placement UNIT (a topic, or its whole
        // co-partition group), each validated on its own and only when one of
        // its partitions is unassigned — a placed topic is never re-validated
        // (its explicit rf may exceed today's placeable brokers), and a unit the
        // balancer refuses is held Unplaceable (logged, counted, retried) instead
        // of failing every other topic's placement.
        //
        // Epoch-restart fix round 1 (M1): the static path only ASSIGNS — it
        // never overwrites an existing assignment (dropAssigned_ in placeUnit_).
        // One-record-per-append invariant: the records go through the gated
        // queue (drainDynamicQueue) — appended in order while this replica
        // leads, kept queued (never dropped) while an append is refused.
        placeEveryUnassignedUnit_();
        drainDynamicQueue();
        assigned_ = true;   // unplaceable units are retried by retryUnplaceable_
    }

    // -----------------------------------------------------------------------
    // Test-only / diagnostic: error counter for BalancerError catch path.
    // Non-zero indicates a misconfigured topic set (RF > nodes, co-partition
    // mismatch, etc.) — no assignment was emitted when this is non-zero.
    // -----------------------------------------------------------------------
    [[nodiscard]] uint64_t balancerErrorCount() const noexcept { return balancerErrors_; }

    // -----------------------------------------------------------------------
    // Kafka AlterPartition (controller side).  A partition leader proposes an
    // ISR change; the controller validates it against the COMMITTED assignment
    // (requester is the current leader at the same leaderEpoch, ISR contains
    // the leader, ⊆ replicas, no other change for that partition in flight) and
    // appends an UpdatePartitionAssignment record (same leader / leaderEpoch,
    // new ISR, partitionEpoch + 1).  Every node applies it; the leader sees it as
    // a same-epoch becomeLeader and starts counting the new committed ISR.
    // Control thread (AeronPartitionTransport handler).  Returns true when a
    // record was enqueued.
    // -----------------------------------------------------------------------
    /// `resp` (optional) is filled when the controller ANSWERS (Kafka
    /// AlterPartitionResponse, MEDIUM-2): an explicit reject, or "already the
    /// committed state".  Returns true when a record was enqueued; `*answer`
    /// tells the caller whether to send `resp` (a transient refusal — another
    /// change for the partition in flight, not the controller — is not
    /// answered: the leader re-sends).
    bool onAlterIsr(const rune::replication::AlterIsrMsg& m,
                    rune::replication::AlterIsrResponseMsg* resp = nullptr,
                    bool* answer = nullptr) {
        namespace rr = rune::replication;
        if (answer) *answer = false;
        auto reply = [&](int16_t code, int32_t partitionEpoch) {
            if (resp == nullptr) return;
            resp->topicId = m.topicId; resp->partition = m.partition;
            resp->leaderId = m.leaderId; resp->leaderEpoch = m.leaderEpoch;
            resp->partitionEpoch = partitionEpoch; resp->errorCode = code;
            if (answer) *answer = true;
        };
        // Controlled shutdown: a NODE-level request (no partition) — recorded
        // by EVERY member (a successor controller already knows the marks);
        // only a ready controller decides / answers.
        if (m.topicId == rr::AlterIsrMsg::kNodeControlTopicId) return onNodeControl_(m, resp, answer);
        // Task 11 fix round 4: a live "cannot serve" report — recorded (even before
        // this controller is ready: a takeover rebuilds the set from fresh reports),
        // never a metadata record.
        if (m.topicId >= 0 && m.isrCount == rr::AlterIsrMsg::kLeaderEpochBump &&
            (m.isr[1] & rr::AlterIsrMsg::kBumpReplicaUnservable) != 0) {
            noteUnservable_(m);
            return false;
        }
        if (!controllerReady_()) return false;
        // Internal topics: only the coordinator partitions on the controller
        // path (MEDIUM-B); __cluster_metadata uses the internal-ISR store.
        if ((m.topicId < 0 && !coordinatorKey_(m.topicId, m.partition)) || m.isrCount < 0) return false;
        const rune::metadata::PartitionAssignment* pa = msm_.assignment(m.topicId, m.partition);
        if (pa == nullptr) return false;
        if (m.isrCount == rr::AlterIsrMsg::kLeaderEpochBump &&
            (m.isr[1] & rr::AlterIsrMsg::kBumpFollowerReport) != 0)
            return onLeaderlessReport_(m, *pa, reply);
        if (m.isrCount == rr::AlterIsrMsg::kLeaderEpochBump &&
            (m.isr[1] & rr::AlterIsrMsg::kLineageReport) != 0)
            return onLineageReport_(m, *pa, reply);
        if (pa->leaderNode != m.leaderId || pa->leaderEpoch != static_cast<int32_t>(m.leaderEpoch)) {
            ++alterIsrRejected_;
            reply(rr::kAlterIsrFencedLeaderEpoch, pa->partitionEpoch);
            return false;   // stale leader / epoch: it will learn the new assignment
        }
        // Compare-and-set on the monotonic partitionEpoch (Kafka; MEDIUM-1): the
        // proposal must have been derived from the CURRENT committed assignment
        // — a stale / re-ordered proposal is never committed, even when the
        // committed ISR went A → B → A meanwhile (no ABA).
        if (m.partitionEpoch != pa->partitionEpoch) {
            ++alterIsrRejected_;
            reply(rr::kAlterIsrInvalidUpdateVersion, pa->partitionEpoch);
            return false;
        }
        if (m.isrCount == rr::AlterIsrMsg::kLeaderEpochBump) return bumpLeaderEpoch_(m, *pa, reply);
        // Review A-I1 (fix round 1): decide — and ANSWER — only on the committed state.  While a record for this
        // partition is between the committed state and the log end (in flight, or queued), a content answer
        // ("nothing to commit" / Ineligible / InvalidRequest) would describe a state the leader's proposal may be
        // about to leave: the leader would clear a proposal that then commits (its HW passing an electable member's
        // LEO).  Stay silent; the leader re-sends (ReplicationMessages.h contract) and gets the committed answer.
        const int64_t key = partitionKey_(m.topicId, m.partition);
        if (inflight_.count(key) != 0) return false;
        for (const auto& q : pendingDynamicRecords_)
            if (q.rec.topicId == m.topicId && q.rec.partition == m.partition) return false;
        std::vector<int32_t> isr;
        isr.reserve(static_cast<std::size_t>(m.isrCount));
        bool hasLeader = false, droppedIneligible = false;
        for (int i = 0; i < m.isrCount; ++i) {
            const int32_t n = m.isr[static_cast<std::size_t>(i)];
            if (std::find(pa->replicas.begin(), pa->replicas.end(), n) == pa->replicas.end()) {
                ++alterIsrRejected_;
                reply(rr::kAlterIsrInvalidRequest, pa->partitionEpoch);
                return false;   // not a replica
            }
            if (n == m.leaderId) hasLeader = true;
            // KIP-841 (MEDIUM-2): never ADD a replica this controller considers
            // dead (fenced) — the fencing pass would remove it again next tick
            // (flapping).  Members already committed are kept (a shrink of them
            // is the leader's call).
            const bool added = std::find(pa->isr.begin(), pa->isr.end(), n) == pa->isr.end();
            if (added && n != m.leaderId &&
                ((eligibleKnown_ && std::find(eligible_.begin(), eligible_.end(), n) == eligible_.end()) ||
                 isShutting_(n))) {   // controlled shutdown: never re-admitted
                droppedIneligible = true;
                continue;
            }
            isr.push_back(n);
        }
        if (!hasLeader) {
            ++alterIsrRejected_;
            reply(rr::kAlterIsrInvalidRequest, pa->partitionEpoch);
            return false;
        }
        {   // nothing to commit: already the committed state (a re-send), or the
            // only change was adding fenced replicas — compared as a SET
            bool same = isr.size() == pa->isr.size();
            for (int32_t n : isr)
                same = same && std::find(pa->isr.begin(), pa->isr.end(), n) != pa->isr.end();
            if (same) {
                if (droppedIneligible) ++alterIsrRejected_;
                reply(droppedIneligible ? rr::kAlterIsrIneligibleReplica : rr::kAlterIsrOk,
                      pa->partitionEpoch);
                return false;
            }
        }
        rune::metadata::BalancerRecord br;
        br.isr      = std::move(isr);
        br.replicas = pa->replicas;
        br.rec.topicId        = m.topicId;
        br.rec.partition      = m.partition;
        br.rec.leaderNode     = pa->leaderNode;
        br.rec.leaderEpoch    = pa->leaderEpoch;
        br.rec.partitionEpoch = pa->partitionEpoch + 1;
        br.rec.isr      = rune::protocol::metadata::Int32ArrayView{br.isr.data(), static_cast<uint16_t>(br.isr.size())};
        br.rec.replicas = rune::protocol::metadata::Int32ArrayView{br.replicas.data(), static_cast<uint16_t>(br.replicas.size())};
        pendingDynamicRecords_.push_back(std::move(br));
        ++alterIsrAccepted_;
        drainDynamicQueue();
        return true;
    }
    /// Leader-epoch restart fence (PartitionReplica::setLeaderEpochFenceFile):
    /// the committed leader `m.leaderId` refused leadership at the committed
    /// epoch (it already led at it in an earlier incarnation — leading again
    /// would write a second history under one epoch).  Kafka parity: every
    /// establishment of leadership gets a NEW epoch, and a restarted ex-leader
    /// (which may have lost acked records — power loss, a memory log) is NOT
    /// re-elected while another live ISR member exists (fix round 1 MAJOR-1).
    /// The request is recorded and decided by the next rebalance tick
    /// (decideBumps_): failover to another live ISR member (co-partition units
    /// together, the requester leaves the ISR), else — the requester is the
    /// sole in-sync replica — the same assignment at a new epoch.  The caller
    /// validated (leader, leaderEpoch, partitionEpoch) against the committed
    /// assignment: a re-send overwrites the same request, a stale one never
    /// reaches here, so no decision is ever taken twice.
    /// Wire: isr[0] = the requester's fence epoch, isr[1] = flags
    /// (AlterIsrMsg::kBumpFenceValid | kBumpMemoryBacked).
    template <typename Reply>
    bool bumpLeaderEpoch_(const rune::replication::AlterIsrMsg& m,
                          const rune::metadata::PartitionAssignment& pa, Reply& /*reply*/) {
        namespace rr = rune::replication;
        BumpRequest r;
        r.requester      = m.leaderId;
        r.leaderEpoch    = pa.leaderEpoch;
        r.partitionEpoch = pa.partitionEpoch;
        r.fence          = (m.isr[1] & rr::AlterIsrMsg::kBumpFenceValid) != 0 ? m.isr[0] : -1;
        r.memoryBacked   = (m.isr[1] & rr::AlterIsrMsg::kBumpMemoryBacked) != 0;
        r.logDirFailed   = (m.isr[1] & rr::AlterIsrMsg::kBumpLogDirFailed) != 0;
        r.leo            = m.isr[2];
        r.incarnation    = incarnationOf_(m);
        noteIncarnation_(m.leaderId, r.incarnation, r.logDirFailed);
        if (r.logDirFailed) markOffline_(m.topicId, m.partition, m.leaderId, r.incarnation);
        bumpRequests_[partitionKey_(m.topicId, m.partition)] = r;
        return false;   // decided (and enqueued) by the next rebalance tick
    }

    /// Controlled shutdown (Kafka parity; KRaft BrokerHeartbeat
    /// wantShutDown).  The stopping node sends kNodeShutdown to EVERY member;
    /// each records the mark of that PROCESS (soft state — a successor
    /// controller already holds it).  The controller acts on it: from the next
    /// rebalance pass the node is avoided as a leader (DynamicBalancerInputs::
    /// avoidLeaders: never preferred, the partitions it leads move to another
    /// live in-sync replica at a new epoch — a partition it is the SOLE
    /// in-sync replica of stays, no unclean election), it leaves the ISR of
    /// every partition it follows and is never re-admitted.  Every OTHER
    /// member that speaks the protocol confirms (kNodeShutdownConfirm) once
    /// its own committed map routes around the node; the controller PUSHES
    /// COMPLETE to the node the moment its own map is clear and the last such
    /// member confirmed (an event; the node's re-send is loss recovery only).
    ///
    /// Process identity (review M1): `isr[3]/isr[4]` carry a per-process
    /// NONCE compared by IDENTITY, never by order.  A request from a nonce
    /// different from the recorded one is a NEW process: it supersedes the
    /// old one (whose mark is cleared — a restart, even with a wiped data dir
    /// or a lower counter); a superseded nonce's late requests are ignored.
    struct NodeLifecycle {
        int64_t nonce          = 0;
        bool    shutting       = false;
        bool    capable        = false;   ///< speaks the protocol (sent us a Register / Shutdown / Confirm)
        bool    userOnly       = false;   ///< the stopping node is the controller (pre-resign view)
        bool    completePushed = false;
        int32_t pushedBits     = -1;      ///< member side: last confirmation sent
        int16_t featureLevel   = -1;      ///< D15: the level this process advertised (-1 = none yet)
        std::array<int64_t, 4> superseded{};   ///< recent dead processes (ring)
        /// E2 residual round 5 (R1): the controller's failure detector found
        /// this node DEAD (out of the heartbeat-eligible set): its process
        /// identity no longer vouches for a transient lineage.  Restored only
        /// by proof of a live process — a NEW process registering (adopt) or
        /// an accepted lineage report from this nonce.
        bool    lineageRevoked = false;
        /// Catalog history Task 11 fix round 0: this PROCESS reported its
        /// CatalogApplier halted (kNodeCatalogHalted) — never a placement target;
        /// cleared only by a new process (adopt).
        bool    catalogHalted  = false;
        uint32_t supersededNext = 0;
        std::unordered_map<int32_t, int32_t> confirmed;   ///< controller side: member → clear bits
        [[nodiscard]] bool isSuperseded(int64_t n) const noexcept {
            for (int64_t x : superseded) if (x != 0 && x == n) return true;
            return false;
        }
        /// A request from process `n`: adopt it as the node's current process.
        /// Returns false when `n` is a dead (superseded) process.
        bool adopt(int64_t n) noexcept {
            if (n == 0 || n == nonce) return true;
            if (isSuperseded(n)) return false;
            if (nonce != 0) superseded[supersededNext++ % superseded.size()] = nonce;
            nonce = n; shutting = false; userOnly = false; completePushed = false; pushedBits = -1;
            lineageRevoked = false;   // R1: a new live process
            catalogHalted  = false;   // Task 11: a new process has a fresh applier
            featureLevel = -1;   // D15: a new process may be another binary
            confirmed.clear();
            return true;
        }
    };
    bool onNodeControl_(const rune::replication::AlterIsrMsg& m,
                        rune::replication::AlterIsrResponseMsg* resp, bool* answer) {
        namespace rr = rune::replication;
        if (m.isrCount != rr::AlterIsrMsg::kLeaderEpochBump || m.leaderId < 0) return false;
        const int64_t nonce = incarnationOf_(m);
        const int32_t flags = m.isr[1];
        if ((flags & rr::AlterIsrMsg::kNodeShutdownConfirm) != 0) {
            // A member's confirmation about stopping node `leaderId` (recorded
            // for that process only); the confirming member speaks the protocol.
            if (m.isr[2] >= 0) nodeLifecycle_[m.isr[2]].capable = true;
            auto it = nodeLifecycle_.find(m.leaderId);
            if (it != nodeLifecycle_.end() && it->second.shutting && it->second.nonce == nonce) {
                it->second.confirmed[m.isr[2]] = m.isr[5];
                if (controllerReady_()) maybePushComplete_(m.leaderId, it->second);   // D1: an event
            }
            return false;
        }
        NodeLifecycle& st = nodeLifecycle_[m.leaderId];
        const bool wasShutting = st.shutting;
        const bool wasHalted   = st.catalogHalted;
        if (!st.adopt(nonce)) {
            // A superseded identity.  Normally a dead process's late frame
            // (nobody acts on the answer); but if a late frame of an UNSEEN
            // older process superseded the LIVE one (fix round 2, N1), the
            // live process gets this answer and rotates to a fresh identity —
            // event-driven recovery, never a silent drop.
            if (resp != nullptr) {
                fillNodeControlResponse_(*resp, m.leaderId, nonce, false);
                resp->errorCode = rr::AlterIsrMsg::kNodeSuperseded;
                if (answer) *answer = true;
            }
            return false;
        }
        st.capable = true;
        if (wasHalted && !st.catalogHalted) {   // Task 11 fix round 1: a new, healthy process
            ++routingInputsEpoch_;
            spdlog::info("MetadataService[{}]: node {} restarted with a healthy catalog — a placement target again",
                         selfId_, m.leaderId);
        }
        st.featureLevel = static_cast<int16_t>(m.isr[rr::AlterIsrMsg::kNodeFeatureLevelSlot]);   // D15
        if (wasShutting && !st.shutting)
            spdlog::info("MetadataService[{}]: node {} restarted (new process) — its shutdown mark is "
                         "cleared", selfId_, m.leaderId);
        const bool shutdown = (flags & rr::AlterIsrMsg::kNodeShutdown) != 0;
        if ((flags & rr::AlterIsrMsg::kNodeCatalogHalted) != 0 && !st.catalogHalted) {
            st.catalogHalted = true;
            ++routingInputsEpoch_;
            spdlog::warn("MetadataService[{}]: node {} reports its __catalog apply HALTED — never a placement "
                         "target{}", selfId_, m.leaderId,
                         m.leaderId == selfId_ ? "; this node places / unplaces no catalog topic" : "");
        }
        if (shutdown && !st.shutting) {
            st.shutting = true;
            ++controlledShutdownRequests_;
            spdlog::info("MetadataService[{}]: node {} is shutting down{}", selfId_, m.leaderId,
                         controllerReady_() ? " — moving its partition leadership to in-sync replicas" : "");
        }
        if (shutdown) {
            const bool userOnly = (flags & rr::AlterIsrMsg::kNodeShutdownUserOnly) != 0;
            if (userOnly != st.userOnly) { st.userOnly = userOnly; st.completePushed = false; }
        }
        if (!controllerReady_()) {
            // A member: (re-)confirm its view to the controller (loss recovery).
            if (st.shutting && m.leaderId != selfId_) { st.pushedBits = -1; queueConfirmation_(m.leaderId, st); }
            return false;
        }
        const bool complete = !st.shutting || shutdownComplete_(m.leaderId, st, st.userOnly);
        if (st.shutting) st.completePushed = complete;   // N5: re-armed when it turns false
        if (resp != nullptr) {
            fillNodeControlResponse_(*resp, m.leaderId, nonce, complete);
            if (answer) *answer = true;
        }
        return false;
    }
    void fillNodeControlResponse_(rune::replication::AlterIsrResponseMsg& r, int32_t node, int64_t nonce,
                                  bool complete) const noexcept {
        namespace rr = rune::replication;
        r.topicId        = rr::AlterIsrMsg::kNodeControlTopicId;
        r.partition      = selfId_;   // the responder
        r.leaderId       = node;
        r.leaderEpoch    = static_cast<rr::Epoch>(static_cast<int32_t>(static_cast<uint32_t>(nonce)));
        r.partitionEpoch = static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint64_t>(nonce) >> 32));
        r.errorCode      = complete ? rr::kAlterIsrOk : rr::AlterIsrMsg::kNodeShutdownPending;
    }
    /// Controller: push COMPLETE to stopping `node` the first time it holds.
    void maybePushComplete_(int32_t node, NodeLifecycle& st) {
        if (!st.shutting) return;
        if (!shutdownComplete_(node, st, st.userOnly)) { st.completePushed = false; return; }   // N5
        if (st.completePushed) return;
        st.completePushed = true;
        rune::replication::AlterIsrResponseMsg r{};
        fillNodeControlResponse_(r, node, st.nonce, true);
        nodeControlRespOut_.emplace_back(node, r);
    }
    /// Member side: the clear bits of `node` in this member's committed map.
    [[nodiscard]] int32_t clearBits_(int32_t node) const noexcept {
        namespace rr = rune::replication;
        const auto& map = msm_.committedMap();
        return (controlledShutdownBlocking(map, node, true) == 0 ? rr::AlterIsrMsg::kConfirmUserClear : 0) |
               (controlledShutdownBlocking(map, node, false) == 0 ? rr::AlterIsrMsg::kConfirmAllClear : 0);
    }
    void queueConfirmation_(int32_t node, NodeLifecycle& st) {
        namespace rr = rune::replication;
        const int32_t bits = clearBits_(node);
        if (bits == st.pushedBits) return;
        st.pushedBits = bits;
        rr::AlterIsrMsg c{};
        c.topicId  = rr::AlterIsrMsg::kNodeControlTopicId;
        c.leaderId = node;
        c.leaderEpoch = 0; c.partitionEpoch = 0;
        c.isrCount = rr::AlterIsrMsg::kLeaderEpochBump;
        c.isr[1] = rr::AlterIsrMsg::kNodeShutdownConfirm;
        c.isr[2] = selfId_;
        c.isr[3] = static_cast<int32_t>(static_cast<uint32_t>(st.nonce));
        c.isr[4] = static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint64_t>(st.nonce) >> 32));
        c.isr[5] = bits;
        nodeControlOut_.push_back(c);
    }
    /// On a committed-map change (cold: only while a node is stopping) —
    /// member: confirm every stopping node whose clear bits changed;
    /// controller: push COMPLETE to every stopping node it now holds for.
    void onMapChangeForShutdown_() {
        if (nodeLifecycle_.empty()) return;
        const uint64_t v = msm_.committedVersion();
        if (v == confirmCheckedVersion_) return;
        confirmCheckedVersion_ = v;
        const bool ctl = controllerReady_();
        for (auto& [n, st] : nodeLifecycle_) {
            if (!st.shutting) continue;
            if (ctl) maybePushComplete_(n, st);
            else if (n != selfId_) queueConfirmation_(n, st);
        }
    }
public:
    /// Controlled shutdown, member side: the consensus leader (controller)
    /// this node routes to changed — re-confirm every stopping node to the new
    /// controller at once (an event; a confirmation routed to the old one is
    /// lost).  Control thread, every cycle (a compare when unchanged).
    void noteController(int32_t controller) {
        if (controller == knownController_) return;
        knownController_ = controller;
        if (controller < 0 || controller == selfId_) return;
        for (auto& [n, st] : nodeLifecycle_)
            if (st.shutting && n != selfId_) { st.pushedBits = -1; queueConfirmation_(n, st); }
    }
    /// Control thread sends these: member confirmations of stopping nodes (to
    /// the current controller) and the controller's pushed COMPLETE answers
    /// (to the stopping node).  Drained by NodeRuntimeAgent every cycle.
    std::vector<rune::replication::AlterIsrMsg>& nodeControlOutbox() noexcept { return nodeControlOut_; }
    std::vector<std::pair<int32_t, rune::replication::AlterIsrResponseMsg>>& nodeControlResponseOutbox() noexcept {
        return nodeControlRespOut_;
    }
    /// Pure: what keeps `node` from stopping in `map` — partitions in whose
    /// ISR it is (or which it leads), EXCEPT one it leads as the SOLE in-sync
    /// replica (cannot move without an unclean election: it stays, and goes
    /// offline with the node — no data loss).  `userOnly`: user topics only
    /// (a stopping CONTROLLER's coordinator partitions move after it resigned).
    [[nodiscard]] static int32_t controlledShutdownBlocking(const rune::metadata::AssignmentMap& map,
                                                            int32_t node, bool userOnly) noexcept {
        int32_t n = 0;
        map.forEach([&](int32_t topicId, int32_t, const rune::metadata::PartitionAssignment& pa) {
            if (userOnly && topicId < 0) return;
            const bool inIsr = std::find(pa.isr.begin(), pa.isr.end(), node) != pa.isr.end();
            const bool soleLeader = pa.leaderNode == node && pa.isr.size() <= 1;
            if ((inIsr || pa.leaderNode == node) && !soleLeader) ++n;
        });
        return n;
    }
    /// The hand-off of `node` is complete for clients: nothing movable is left
    /// on it in THIS controller's committed map, and every other live member
    /// confirmed the same of ITS committed map (its routing no longer names
    /// `node`).  `userOnly`: a stopping controller, before it resigns.
    bool shutdownComplete_(int32_t node, NodeLifecycle& st, bool userOnly) {
        namespace rr = rune::replication;
        // The user-only scope is the STOPPING CONTROLLER's pre-resign view: only this controller judging its own
        // shutdown uses it.  A successor holds the mark the old controller's Draining broadcasts left (userOnly)
        // — judged on user partitions alone it confirmed COMPLETE while members still routed the coordinator
        // partitions to the stopping node.  Another node's shutdown is always judged on the full scope.
        userOnly = userOnly && node == selfId_;
        if (controlledShutdownBlocking(msm_.committedMap(), node, userOnly) != 0) return false;
        // A controller that has not learnt the live set yet (no rebalance tick since it became controller — a
        // successor right after the old controller resigned) cannot know whom to wait for: an empty eligible_
        // would confirm COMPLETE before any member routes around the node.  The first tick re-checks (below).
        if (!eligibleKnown_) return false;
        const int32_t need = userOnly ? rr::AlterIsrMsg::kConfirmUserClear : rr::AlterIsrMsg::kConfirmAllClear;
        for (int32_t n : eligible_) {
            if (n == selfId_ || n == node || isShutting_(n)) continue;
            // D4: only members that speak the protocol (an older binary mid-
            // roll never confirms — it is not waited for).
            const auto ls = nodeLifecycle_.find(n);
            if (ls == nodeLifecycle_.end() || !ls->second.capable) continue;
            const auto c = st.confirmed.find(n);
            if (c == st.confirmed.end() || (c->second & need) == 0) return false;
        }
        return true;
    }
    [[nodiscard]] bool isShutting_(int32_t node) const noexcept {
        const auto it = nodeLifecycle_.find(node);
        return it != nodeLifecycle_.end() && it->second.shutting;
    }
    /// The nodes in controlled shutdown (reused scratch; control plane).
    const std::vector<int32_t>& shuttingNodes_() {
        shuttingScratch_.clear();
        for (const auto& [n, st] : nodeLifecycle_) if (st.shutting) shuttingScratch_.push_back(n);
        return shuttingScratch_;
    }
    [[nodiscard]] uint64_t controlledShutdownRequestsForTest() const noexcept { return controlledShutdownRequests_; }

    /// Kafka broker fencing: a node the cluster considers dead leaves the
    /// COMMITTED ISR of every partition whose leader is alive (failover
    /// records handle dead leaders) — with HW = min over the ISR it would
    /// otherwise hold the HW for replicaLagMaxMs.  It re-enters through its
    /// leader's AlterIsr once it fetched back to the HW.  Controlled shutdown:
    /// a stopping FOLLOWER leaves the ISR now too (Kafka removes a shutting-
    /// down broker from every ISR).  Partitions with a record in flight /
    /// queued / in `recs` are skipped.  Control plane.
    void fenceIsrs_(const std::vector<rune::metadata::NodeId>& eligibleNodes,
                    std::vector<rune::metadata::BalancerRecord>& recs) {
        msm_.committedMap().forEach([&](int32_t topicId, int32_t partition,
                                        const rune::metadata::PartitionAssignment& pa) {
            if (pa.leaderNode == rune::metadata::NO_LEADER) return;
            if (std::find(eligibleNodes.begin(), eligibleNodes.end(), pa.leaderNode) == eligibleNodes.end()) return;
            const int64_t key = partitionKey_(topicId, partition);
            if (inflight_.count(key) != 0) return;
            for (const auto& r : recs) if (r.rec.topicId == topicId && r.rec.partition == partition) return;
            for (const auto& q : pendingDynamicRecords_)
                if (q.rec.topicId == topicId && q.rec.partition == partition) return;
            auto alive = [&](int32_t n) {
                return n == pa.leaderNode ||
                       (std::find(eligibleNodes.begin(), eligibleNodes.end(), n) != eligibleNodes.end() &&
                        !isShutting_(n));
            };
            bool anyDead = false;   // steady state: no allocation
            for (int32_t n : pa.isr) anyDead = anyDead || !alive(n);
            if (!anyDead) return;
            std::vector<int32_t> isr;
            for (int32_t n : pa.isr)
                if (alive(n)) isr.push_back(n);
            rune::metadata::BalancerRecord br;
            br.isr      = std::move(isr);
            br.replicas = pa.replicas;
            br.rec.topicId        = topicId;
            br.rec.partition      = partition;
            br.rec.leaderNode     = pa.leaderNode;
            br.rec.leaderEpoch    = pa.leaderEpoch;
            br.rec.partitionEpoch = pa.partitionEpoch + 1;
            br.rec.isr      = rune::protocol::metadata::Int32ArrayView{br.isr.data(), static_cast<uint16_t>(br.isr.size())};
            br.rec.replicas = rune::protocol::metadata::Int32ArrayView{br.replicas.data(), static_cast<uint16_t>(br.replicas.size())};
            recs.push_back(std::move(br));
        });
    }

    static int64_t incarnationOf_(const rune::replication::AlterIsrMsg& m) noexcept {
        return static_cast<int64_t>((static_cast<uint64_t>(static_cast<uint32_t>(m.isr[4])) << 32) |
                                    static_cast<uint32_t>(m.isr[3]));
    }
    /// N2: a node's new process incarnation invalidates its soft position
    /// reports (its log may be gone — a memory log, a power loss) and, its
    /// log dir being healthy again, its offline marks (N1 re-registration).
    void noteIncarnation_(int32_t node, int64_t inc, bool logDirFailed) {
        if (inc == 0) return;   // untracked
        auto it = nodeIncarnation_.find(node);
        if (it != nodeIncarnation_.end() && it->second == inc) return;
        nodeIncarnation_[node] = inc;
        positions_.dropNode(node);
        if (!logDirFailed)
            std::erase_if(offlineReplicas_, [&](const OfflineReplica& o) {
                return o.node == node && o.incarnation != inc;
            });
    }
    void markOffline_(int32_t topicId, int32_t partition, int32_t node, int64_t inc) {
        for (auto& o : offlineReplicas_)
            if (o.topicId == topicId && o.partition == partition && o.node == node) { o.incarnation = inc; return; }
        offlineReplicas_.push_back({topicId, partition, node, inc});
    }
    [[nodiscard]] bool isOffline_(int32_t topicId, int32_t partition, int32_t node) const noexcept {
        for (const auto& o : offlineReplicas_)
            if (o.topicId == topicId && o.partition == partition && o.node == node) return true;
        return false;
    }

    // -----------------------------------------------------------------------
    // E2 final fix wave (C1 multi-node, USER DECISION option b): TRANSIENT
    // partition data lineages.  A transient leader reports, for its leadership,
    // the leader epoch its ring's data came from (-1: unborn).  The controller
    // KEEPS the committed lineage when the data provably belongs to it, MINTS a
    // fresh one otherwise (decideTransientLineage) and commits a
    // TransientLineageRecord confirming it for that leadership; the report is
    // answered kAlterIsrLineageConfirmed only once that record is COMMITTED
    // (the leader re-sends until then — a controller failover re-decides from
    // the committed map).  Single node (setTransientLineageAuthority): no
    // replica reports — every transient ring is born with the process, so the
    // controller confirms each transient partition itself, keeping the lineage
    // only within the same process incarnation.
    // -----------------------------------------------------------------------
    template <typename Reply>
    bool onLineageReport_(const rune::replication::AlterIsrMsg& m,
                          const rune::metadata::PartitionAssignment& pa, Reply& reply) {
        namespace rr = rune::replication;
        if (pa.leaderNode != m.leaderId || pa.leaderEpoch != static_cast<int32_t>(m.leaderEpoch)) {
            reply(rr::kAlterIsrFencedLeaderEpoch, pa.partitionEpoch);   // not this leadership any more
            return false;
        }
        if (auto ls = nodeLifecycle_.find(m.leaderId);   // R1: the current leadership's live process reports
            ls != nodeLifecycle_.end() && ls->second.lineageRevoked && ls->second.nonce == incarnationOf_(m))
            ls->second.lineageRevoked = false;
        const rune::metadata::TransientLineage* rec = msm_.lineage(m.topicId, m.partition);
        if (rune::metadata::lineageConfirmedFor(rec, &pa, incarnationOf_(m))) {   // N1: by THIS process
            reply(rr::kAlterIsrLineageConfirmed, pa.partitionEpoch);
            return false;
        }
        const int64_t key = partitionKey_(m.topicId, m.partition);
        if (lineageInflight_.count(key) != 0) return false;   // being committed: the leader re-sends
        for (const auto& q : pendingLineageRecords_)
            if (q.topicId == m.topicId && q.partition == m.partition) return false;   // queued
        rune::metadata::TransientLineageRecord r;
        r.topicId   = m.topicId;
        r.partition = m.partition;
        // N1: the record carries the reporting leader's process identity.
        r.l = rune::metadata::decideTransientLineage(rec, m.leaderId, pa.leaderEpoch, m.isr[2], mintLineage_(),
                                                     incarnationOf_(m), pa.cleanElection);   // N3: unclean mints
        noteLineageDecision_(r, rec);
        pendingLineageRecords_.push_back(r);
        drainDynamicQueue();
        return true;
    }
    void noteLineageDecision_(const rune::metadata::TransientLineageRecord& r,
                              const rune::metadata::TransientLineage* prev) {
        const bool minted = prev == nullptr || prev->lineage != r.l.lineage;
        ++(minted ? lineagesMinted_ : lineagesKept_);
        spdlog::info("MetadataService[{}]: transient topicId={} partition={} leader {} epoch {}: data lineage {} "
                     "{:#x} (minted at epoch {})", selfId_, r.topicId, r.partition, r.l.confirmedLeader,
                     r.l.confirmedEpoch, minted ? "MINTED" : "kept", r.l.lineage, r.l.mintEpoch);
    }
    uint64_t mintLineage_() noexcept {
        // splitmix64 over (time, counter, node): unique per decision, non-zero.
        uint64_t x = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()) ^
                     (++lineageMintCounter_ << 32) ^ static_cast<uint64_t>(static_cast<uint32_t>(selfId_)) ^
                     lineageSalt_;
        x += 0x9E3779B97F4A7C15ull;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
        x ^= x >> 31;
        return x == 0 ? 1 : x;
    }
public:
    /// Single node (C1): this controller is the transient lineage authority —
    /// `selfIncarnation` (non-zero, per process) and the transient-topic
    /// predicate.  Control thread; before the first doWork.
    void setTransientLineageAuthority(int64_t selfIncarnation, bool (*isTransient)(void*, int32_t), void* ctx) noexcept {
        lineageAuthorityIncarnation_ = selfIncarnation;
        lineageIsTransient_          = isTransient;
        lineageIsTransientCtx_       = ctx;
        lineageSalt_ ^= static_cast<uint64_t>(selfIncarnation);
    }
    [[nodiscard]] const rune::metadata::TransientLineage* transientLineage(int32_t topicId, int32_t partition) const noexcept {
        return msm_.lineage(topicId, partition);
    }
    /// E2 C1 (residual N1): the data lineage the group coordinator may answer
    /// committed offsets of a TRANSIENT partition against, or
    /// kLineageUntrusted (the settling window: answered NotLeader).  Trusted
    /// only when confirmed for the CURRENT committed leadership AND by the
    /// leader's LIVE process: single node — this process (the lineage
    /// authority's incarnation); multi-node — the identity this node currently
    /// knows for the confirmed leader (its registration nonce; every node
    /// registers with every controller).  After a full-cluster restart the
    /// committed assignment still names the dead leadership until its bump /
    /// failover commits: its committed identity no longer matches (or is not
    /// yet known) — never trusted.  Runtime thread.
    [[nodiscard]] uint64_t trustedTransientLineage(int32_t topicId, int32_t partition) const noexcept {
        const rune::metadata::TransientLineage* rec = msm_.lineage(topicId, partition);
        if (rec == nullptr) return rune::metadata::kLineageUntrusted;
        const int64_t live = lineageAuthorityIncarnation_ != 0 ? lineageAuthorityIncarnation_
                                                               : knownProcessIdentity(rec->confirmedLeader);
        if (live == 0) return rune::metadata::kLineageUntrusted;   // the leader has not (re)registered yet
        return rune::metadata::lineageConfirmedFor(rec, msm_.assignment(topicId, partition), live)
                   ? rec->lineage : rune::metadata::kLineageUntrusted;
    }
    /// TEST-ONLY: see NodeConfig::testUserDecisionHoldMs.
    void setUserDecisionHoldForTest(int64_t ns) noexcept { userDecisionHoldNs_ = ns; }
    /// Catalog history Task 22 seam (CONTROL thread, via onControlThreadForTest): while held a runtime topic is
    /// recorded but its initial placement is not emitted (the window between "applied" and "placement committed" of
    /// the 3-node slice's B1 test); releasing it places every unplaced unit at once.  Never set in production.
    /// Scope (Task 22 fix round 1, M8): the hold covers EVERY initial-placement path - addRuntimeTopic,
    /// placeEveryUnassignedUnit_ (controller catch-up) and retryUnplaceable_ - so a held topic is never placed by
    /// another route; it does not touch re-placement of an already-placed topic.
    void holdPlacementForTest(bool on) {
        placementHeldForTest_ = on;
        if (!on && controllerReady_() && !liveNodes_.empty() && !catalogHalted(selfId_)) {
            placeEveryUnassignedUnit_();
            drainDynamicQueue();
        }
    }
    /// E2 C1 residual N1: the process identity this node currently knows for
    /// `node` (its node-control registration nonce; every node registers with
    /// every new controller) — 0 when unknown.
    [[nodiscard]] int64_t knownProcessIdentity(int32_t node) const noexcept {
        const auto it = nodeLifecycle_.find(node);
        if (it == nodeLifecycle_.end() || it->second.lineageRevoked) return 0;   // R1: found dead
        return it->second.nonce;
    }
    /// R1 (round 5): moves whenever a committed lineage changes (the group
    /// coordinator re-sweeps its groups for superseded positions).
    [[nodiscard]] uint64_t transientLineageVersion() const noexcept { return msm_.lineageVersion(); }
    [[nodiscard]] uint64_t lineageIdentitiesRevokedForTest() const noexcept { return lineageIdentitiesRevoked_; }
    [[nodiscard]] const rune::metadata::PartitionAssignment* committedAssignment(int32_t topicId,
                                                                                int32_t partition) const noexcept {
        return msm_.assignment(topicId, partition);
    }
    [[nodiscard]] uint64_t lineagesMintedForTest() const noexcept { return lineagesMinted_; }
    [[nodiscard]] uint64_t lineagesKeptForTest() const noexcept { return lineagesKept_; }
private:
    /// Single node: confirm every transient partition this node leads (keep
    /// within this process, mint across processes) — re-checked when the
    /// committed map or lineages change.  Cold.
    void maybeConfirmSingleNodeLineages_() {
        if (lineageAuthorityIncarnation_ == 0 || lineageIsTransient_ == nullptr || !controllerReady_()) return;
        if (msm_.committedVersion() == lineageScanMapVersion_ && msm_.lineageVersion() == lineageScanLineageVersion_ &&
            pendingLineageRecords_.empty() && lineageInflight_.empty())
            return;
        lineageScanMapVersion_     = msm_.committedVersion();
        lineageScanLineageVersion_ = msm_.lineageVersion();
        msm_.committedMap().forEach([&](int32_t t, int32_t p, const rune::metadata::PartitionAssignment& pa) {
            if (t < 0 || pa.leaderNode != selfId_ || !lineageIsTransient_(lineageIsTransientCtx_, t)) return;
            const rune::metadata::TransientLineage* rec = msm_.lineage(t, p);
            if (rune::metadata::lineageConfirmedFor(rec, &pa, lineageAuthorityIncarnation_)) return;
            const int64_t key = partitionKey_(t, p);
            if (lineageInflight_.count(key) != 0) return;
            for (const auto& q : pendingLineageRecords_) if (q.topicId == t && q.partition == p) return;
            // Same process: the ring (and its data) is the one confirmed before.
            const int32_t src = rec != nullptr && rec->incarnation == lineageAuthorityIncarnation_ ? rec->confirmedEpoch : -1;
            rune::metadata::TransientLineageRecord r;
            r.topicId = t; r.partition = p;
            r.l = rune::metadata::decideTransientLineage(rec, selfId_, pa.leaderEpoch, src, mintLineage_(),
                                                         lineageAuthorityIncarnation_);
            noteLineageDecision_(r, rec);
            pendingLineageRecords_.push_back(r);
        });
    }

public:
    /// N1/N2: a replica of a LEADERLESS partition reports itself.  Recorded
    /// only for the current leaderless epoch; always answered (a stale report
    /// is moot — the replica gets a newer role).
    template <typename Reply>
    bool onLeaderlessReport_(const rune::replication::AlterIsrMsg& m,
                             const rune::metadata::PartitionAssignment& pa, Reply& reply) {
        namespace rr = rune::replication;
        const bool failed = (m.isr[1] & rr::AlterIsrMsg::kBumpLogDirFailed) != 0;
        const int64_t inc = incarnationOf_(m);
        noteIncarnation_(m.leaderId, inc, failed);
        if (failed) markOffline_(m.topicId, m.partition, m.leaderId, inc);
        const bool replica = std::find(pa.replicas.begin(), pa.replicas.end(), m.leaderId) != pa.replicas.end();
        if (replica && pa.leaderNode == rune::metadata::NO_LEADER && m.leaderEpoch == pa.leaderEpoch)
            leaderlessReports_[partitionKey_(m.topicId, m.partition)][m.leaderId] =
                FreshReport{m.isr[2], failed, pa.leaderEpoch};
        reply(rr::kAlterIsrOk, pa.partitionEpoch);
        return false;
    }

    /// N2: decide the leader of every partition made leaderless because its
    /// memory-backed sole-ISR leader restarted empty — only on FRESH reports
    /// (this leaderless epoch) of EVERY live replica (an event: the last one
    /// arriving).  The most complete live replica leads: an ISR member when
    /// none holds more (clean); a non-ISR replica holding more only with
    /// survivor (unclean) election allowed — else the partition stays
    /// leaderless (Kafka: offline, ISR lineage kept).
    ///
    /// D15 item 2: decided per CO-PARTITION UNIT (same-index partitions of a
    /// co-partition group): ALL its leaderless members (whatever made them
    /// leaderless — review m3) are decided together, once every one has its
    /// fresh reports: ONE leader clean for all of them when one exists
    /// (preferring a live sibling's leader — colocated); else (review M2:
    /// data safety beats colocation) each member gets its own clean leader and
    /// the preferred-leader pass restores colocation later; survivor
    /// (unclean, opt-in) election only for a member with no clean candidate.
    /// The balancer leaves such a unit to this pass (runDynamicRebalanceOnTick).
    void decideLeaderless_(std::vector<rune::metadata::BalancerRecord>& recs, int maxRecords) {
        namespace rm = rune::metadata;
        // Prune superseded entries (a newer committed decision).
        for (auto it = emptyMemoryLeader_.begin(); it != emptyMemoryLeader_.end();) {
            const auto* pa = msm_.assignment(static_cast<int32_t>(it->first >> 32),
                                             static_cast<int32_t>(static_cast<uint32_t>(it->first)));
            if (pa != nullptr && pa->leaderEpoch < it->second.leaderlessEpoch) { ++it; continue; }   // in flight
            if (pa == nullptr || pa->leaderNode != rm::NO_LEADER || pa->leaderEpoch != it->second.leaderlessEpoch) {
                leaderlessReports_.erase(it->first);
                heldLeaderless_.erase(it->first);
                it = emptyMemoryLeader_.erase(it);
                continue;
            }
            ++it;
        }
        unitVisited_.clear();
        unitDecided_.clear();
        auto busy = [&](int64_t key) {
            const int32_t t = static_cast<int32_t>(key >> 32);
            const int32_t p = static_cast<int32_t>(static_cast<uint32_t>(key));
            if (inflight_.count(key) != 0) return true;
            for (const auto& r : recs) if (r.rec.topicId == t && r.rec.partition == p) return true;
            for (const auto& q : pendingDynamicRecords_) if (q.rec.topicId == t && q.rec.partition == p) return true;
            return false;
        };
        for (const auto& [key0, e0] : emptyMemoryLeader_) {
            if (std::find(unitVisited_.begin(), unitVisited_.end(), key0) != unitVisited_.end()) continue;
            unitKeys_(key0, unitScratch_);
            for (int64_t k : unitScratch_) unitVisited_.push_back(k);
            // The unit's LEADERLESS members (review m3: whatever made them
            // leaderless — an empty memory leader, an offline log dir, a dead
            // ISR) and the live leader of a sibling that has one.
            int32_t siblingLeader = -1;
            bool ready = true;
            struct View { int64_t key; const rm::PartitionAssignment* pa; const std::unordered_map<int32_t, FreshReport>* reps; int32_t maxLeo; };
            std::vector<View> members;   // cold path (a leaderless decision)
            for (int64_t k : unitScratch_) {
                const auto* pa = msm_.assignment(static_cast<int32_t>(k >> 32), static_cast<int32_t>(static_cast<uint32_t>(k)));
                if (pa == nullptr) continue;
                if (pa->leaderNode != rm::NO_LEADER) {
                    if (isEligible_(pa->leaderNode) && !isShutting_(pa->leaderNode)) siblingLeader = pa->leaderNode;
                    continue;
                }
                const auto e = emptyMemoryLeader_.find(k);
                if ((e != emptyMemoryLeader_.end() && pa->leaderEpoch < e->second.leaderlessEpoch) || busy(k)) { ready = false; continue; }
                // Wait for the fresh report (THIS leaderless epoch) of every live replica.
                const auto& reps = leaderlessReports_[k];
                for (int32_t n : pa->replicas) {
                    if (!isEligible_(n)) continue;
                    // Task 11 fix round 5 (I2): a replica that reports it cannot
                    // serve has no PartitionReplica — it never sends a follower
                    // report: never waited for (and never a candidate, below).
                    if (reportedUnservable(static_cast<int32_t>(k >> 32), static_cast<int32_t>(static_cast<uint32_t>(k)), n))
                        continue;
                    const auto r = reps.find(n);
                    if (r == reps.end() || r->second.epoch != pa->leaderEpoch) { ready = false; break; }
                }
                members.push_back(View{k, pa, &reps, -1});
            }
            if (!ready || members.empty()) continue;
            // Over the remaining budget: the next tick decides it, as a unit (the
            // runtime re-ticks on the very next cycle after any emit — SlowTick).
            // D15b l2: an EMPTY tick always admits it — a unit larger than the
            // whole budget otherwise never fit and stayed leaderless forever
            // (the drain stays bounded by kMaxRecordsPerBatch).
            if (!recs.empty() && static_cast<int>(recs.size() + members.size()) > maxRecords) continue;
            auto avail = [&](const View& v, int32_t n, int32_t* leo) {
                if (!isEligible_(n) || isShutting_(n)) return false;
                if (reportedUnservable(static_cast<int32_t>(v.key >> 32),
                                       static_cast<int32_t>(static_cast<uint32_t>(v.key)), n))
                    return false;   // fix round 5 (I2): never a candidate
                if (std::find(v.pa->replicas.begin(), v.pa->replicas.end(), n) == v.pa->replicas.end()) return false;
                const auto r = v.reps->find(n);
                if (r == v.reps->end() || r->second.logDirFailed || r->second.epoch != v.pa->leaderEpoch) return false;
                if (isOffline_(static_cast<int32_t>(v.key >> 32), static_cast<int32_t>(static_cast<uint32_t>(v.key)), n)) return false;
                if (leo) *leo = r->second.leo;
                return true;
            };
            for (auto& v : members)
                for (int32_t n : v.pa->replicas) {
                    int32_t leo = -1;
                    if (avail(v, n, &leo)) v.maxLeo = std::max(v.maxLeo, leo);
                }
            // Clean: an available ISR member nobody available holds more than.
            auto clean = [&](const View& v, int32_t n) {
                int32_t leo = -1;
                return avail(v, n, &leo) && leo >= v.maxLeo &&
                       std::find(v.pa->isr.begin(), v.pa->isr.end(), n) != v.pa->isr.end();
            };
            auto cleanForAll = [&](int32_t n) {
                for (const auto& v : members) if (!clean(v, n)) return false;
                return true;
            };
            // 1. One leader clean for EVERY member (colocated, preferring a
            //    live sibling's leader).
            int32_t common = -1;
            if (siblingLeader >= 0 && cleanForAll(siblingLeader)) common = siblingLeader;
            for (std::size_t i = 0; common < 0 && i < members[0].pa->replicas.size(); ++i)
                if (cleanForAll(members[0].pa->replicas[i])) common = members[0].pa->replicas[i];
            // 2. Review M2 (data safety beats colocation): otherwise each member
            //    gets ITS OWN clean leader (preferring one already chosen in the
            //    unit, then the sibling's) — the preferred-leader pass restores
            //    colocation later.  3. Survivor (unclean) election only for a
            //    member with NO clean candidate at all (opt-in), else it is held.
            std::vector<int32_t> chosen;   // cold
            for (const auto& v : members) {
                int32_t leader = -1;
                bool unclean = false;
                if (common >= 0) {
                    leader = common;
                } else {
                    for (int32_t n : chosen) if (leader < 0 && clean(v, n)) leader = n;
                    if (leader < 0 && siblingLeader >= 0 && clean(v, siblingLeader)) leader = siblingLeader;
                    for (int32_t n : v.pa->replicas) if (leader < 0 && clean(v, n)) leader = n;
                    if (leader < 0 && allowSurvivorElection_) {
                        int32_t bestLeo = -1;
                        for (int32_t n : v.pa->replicas) {
                            int32_t leo = -1;
                            if (avail(v, n, &leo) && leo > bestLeo) { bestLeo = leo; leader = n; }
                        }
                        unclean = leader >= 0;
                    }
                }
                if (leader < 0) {
                    // Held offline (Kafka: unclean election disabled, or nobody
                    // available): the marker stays so the balancer never
                    // re-elects the empty leader; no record (no churn).
                    if (heldLeaderless_.insert(v.key).second) ++leaderlessHeld_;
                    continue;
                }
                if (std::find(chosen.begin(), chosen.end(), leader) == chosen.end()) chosen.push_back(leader);
                rm::BalancerRecord br;
                br.replicas = v.pa->replicas;
                br.rec.topicId     = static_cast<int32_t>(v.key >> 32);
                br.rec.partition   = static_cast<int32_t>(static_cast<uint32_t>(v.key));
                br.rec.leaderNode  = leader;
                br.rec.leaderEpoch = v.pa->leaderEpoch + 1;
                br.rec.partitionEpoch = std::max(v.pa->partitionEpoch + 1, br.rec.leaderEpoch);
                if (unclean) {
                    br.isr = {leader};                               // unclean, opt-in
                } else {
                    br.isr.push_back(leader);                        // clean: nobody holds more
                    for (int32_t n : v.pa->isr) if (n != leader) br.isr.push_back(n);
                }
                br.rec.isr      = rune::protocol::metadata::Int32ArrayView{br.isr.data(), static_cast<uint16_t>(br.isr.size())};
                br.rec.replicas = rune::protocol::metadata::Int32ArrayView{br.replicas.data(), static_cast<uint16_t>(br.replicas.size())};
                recs.push_back(std::move(br));
                unitDecided_.push_back(v.key);
            }
        }
        for (int64_t k : unitDecided_) {
            leaderlessReports_.erase(k);
            heldLeaderless_.erase(k);
            emptyMemoryLeader_.erase(k);
        }
    }
    /// R1 (round 5): a node the failure detector found dead (out of the
    /// heartbeat-eligible set) no longer vouches for a lineage — its remembered
    /// identity is cleared for trust.  (Its leaderships are failed over / made
    /// leaderless in the same tick; a node wrongly found dead gets a new
    /// leadership whose lineage report restores it.)
    void revokeDeadLineageIdentities_() {
        for (auto& [n, st] : nodeLifecycle_) {
            if (n == selfId_ || st.nonce == 0 || st.lineageRevoked || isEligible_(n)) continue;
            st.lineageRevoked = true;
            ++lineageIdentitiesRevoked_;
            spdlog::info("MetadataService[{}]: node {} found dead — its process identity no longer vouches for "
                         "transient lineages", selfId_, n);
        }
    }
    uint64_t lineageIdentitiesRevoked_ = 0;
    uint64_t catalogPlacementsSkippedHalted_ = 0;   // Task 11: a halted controller's skipped placements / removals
    // Task 11 fix round 1 (I1/I2): the explicit Unplaceable state (control thread).
    std::map<int32_t, UnplaceableTopic> unplaceable_;
    std::vector<int32_t>                haltedScratch_;   // M2: reused (capacity persists)
    // Task 11 fix round 4: live "cannot serve" reports + the BLOCKED partitions derived from them.
    std::vector<UnservableReport>       unservable_;
    std::vector<rune::metadata::DynamicBalancerInputs::LeaderExclusion> electionExcludedScratch_;
    int64_t                             firstPassNs_    = -1;   // I1: this process's first doWork
    uint64_t                            warmupLogged_   = 0;
    int64_t                             unservableWarmupNs_ = 0;  // I1: 0 = off (unit rigs)
    std::vector<int64_t>                blocked_;
    std::vector<int64_t>                blockedScratch_;
    uint64_t                            unplaceableEvents_     = 0;
    uint64_t                            unplaceableTriedEpoch_ = ~0ull;
    [[nodiscard]] bool isEligible_(int32_t n) const noexcept {
        return std::find(eligible_.begin(), eligible_.end(), n) != eligible_.end();
    }
    /// D15 item 2: the keys of `key`'s co-partition unit (same-index
    /// partitions of every topic in its co-partition group; itself alone when
    /// it has none).
    void unitKeys_(int64_t key, std::vector<int64_t>& out) const {
        out.clear();
        const int32_t t = static_cast<int32_t>(key >> 32);
        const int32_t p = static_cast<int32_t>(static_cast<uint32_t>(key));
        const rune::metadata::TopicSpec* spec = nullptr;
        for (const auto& ts : configuredTopics_) if (ts.topicId == t) { spec = &ts; break; }
        if (spec == nullptr || spec->coPartitionGroup.empty()) { out.push_back(key); return; }
        for (const auto& ts : configuredTopics_)
            if (ts.coPartitionGroup == spec->coPartitionGroup && p < ts.partitionCount)
                out.push_back(partitionKey_(ts.topicId, p));
    }

    /// MAJOR-1: decide the recorded bump requests.  Called by the rebalance
    /// tick AFTER the balancer ran with `excludedLeaders` (requests whose key
    /// got a record there failed over).  The rest — no other live in-sync
    /// member — are re-elected at a new epoch; a MEMORY-backed requester (its
    /// log is empty after the restart) yields instead to the live replica that
    /// reported the most records, if any holds more than it (never wipe
    /// followers to an empty leader).
    void decideBumps_(std::vector<rune::metadata::BalancerRecord>& recs, int maxRecords,
                      const std::vector<rune::metadata::DynamicBalancerInputs::LeaderExclusion>& offered) {
        for (auto it = bumpRequests_.begin(); it != bumpRequests_.end();) {
            const int32_t topicId   = static_cast<int32_t>(it->first >> 32);
            const int32_t partition = static_cast<int32_t>(static_cast<uint32_t>(it->first));
            bool isOffered = false;   // busy (in flight / queued) ones wait for the next tick
            for (const auto& o : offered) isOffered = isOffered || (o.topicId == topicId && o.partition == partition);
            if (!isOffered) { ++it; continue; }
            bool decided = false;
            for (const auto& r : recs) decided = decided || (r.rec.topicId == topicId && r.rec.partition == partition);
            if (decided) { ++bumpFailovers_; it = bumpRequests_.erase(it); continue; }
            if (static_cast<int>(recs.size()) >= maxRecords) { ++it; continue; }   // next tick
            const rune::metadata::PartitionAssignment* pa = msm_.assignment(topicId, partition);
            if (pa == nullptr) { it = bumpRequests_.erase(it); continue; }
            const BumpRequest& q = it->second;
            rune::metadata::BalancerRecord br;
            br.replicas = pa->replicas;
            br.rec.topicId        = topicId;
            br.rec.partition      = partition;
            br.rec.partitionEpoch = pa->partitionEpoch + 1;
            // Coordinator partitions are not in the balancer's topic set: the
            // same rule here — another live ISR member leads, the requester
            // leaves the ISR (re-admitted by the new leader's AlterIsr).
            if (topicId < 0) {
                for (int32_t n : pa->isr) {
                    if (n == q.requester || isShutting_(n)) continue;
                    if (std::find(eligible_.begin(), eligible_.end(), n) == eligible_.end()) continue;
                    br.rec.leaderNode  = n;
                    br.rec.leaderEpoch = pa->leaderEpoch + 1;
                    br.isr.push_back(n);
                    for (int32_t o : pa->isr) if (o != n && o != q.requester) br.isr.push_back(o);
                    break;
                }
            }
            if (!br.isr.empty()) {
                ++bumpFailovers_;   // coordinator failover decided above
            } else if (q.logDirFailed || (q.memoryBacked && q.leo == 0)) {
                // N1: an offline log dir is never re-granted; N2: an EMPTY
                // memory leader is not re-elected blind — the partition goes
                // leaderless (Kafka offline, ISR lineage kept); the memory case
                // is then decided on fresh reports (decideLeaderless_).
                br.rec.leaderNode  = rune::metadata::NO_LEADER;
                br.rec.leaderEpoch = pa->leaderEpoch + 1;
                br.isr             = pa->isr;
                // D15 item 1: WHY it is leaderless is committed with the record
                // (a successor controller re-derives the exclusion from it).
                br.mark.kind = q.logDirFailed ? rune::metadata::LeaderlessMark::Kind::LogDirOffline
                                              : rune::metadata::LeaderlessMark::Kind::EmptyMemoryLeader;
                br.mark.node        = q.requester;
                br.mark.incarnation = q.incarnation;
                if (!q.logDirFailed) {
                    emptyMemoryLeader_[it->first] = EmptyMemoryLeader{q.requester, br.rec.leaderEpoch, q.incarnation};
                    leaderlessReports_.erase(it->first);
                }
                ++leaderlessHeld_;
            } else {
                br.rec.leaderNode  = pa->leaderNode;
                br.rec.leaderEpoch = std::max(pa->leaderEpoch, q.fence) + 1;   // L2: past the fence at once
                br.isr             = pa->isr;
                ++leaderEpochBumps_;
            }
            if (br.rec.partitionEpoch < br.rec.leaderEpoch) br.rec.partitionEpoch = br.rec.leaderEpoch;
            br.rec.isr      = rune::protocol::metadata::Int32ArrayView{br.isr.data(), static_cast<uint16_t>(br.isr.size())};
            br.rec.replicas = rune::protocol::metadata::Int32ArrayView{br.replicas.data(), static_cast<uint16_t>(br.replicas.size())};
            recs.push_back(std::move(br));
            it = bumpRequests_.erase(it);
        }
    }

    /// Requests still matching the committed assignment and decidable now.
    void validBumpExclusions_(std::vector<rune::metadata::DynamicBalancerInputs::LeaderExclusion>& out) {
        out.clear();
        for (auto it = bumpRequests_.begin(); it != bumpRequests_.end();) {
            const int32_t topicId   = static_cast<int32_t>(it->first >> 32);
            const int32_t partition = static_cast<int32_t>(static_cast<uint32_t>(it->first));
            const auto* pa = msm_.assignment(topicId, partition);
            const BumpRequest& q = it->second;
            if (pa == nullptr || pa->leaderNode != q.requester || pa->leaderEpoch != q.leaderEpoch ||
                pa->partitionEpoch != q.partitionEpoch) {
                it = bumpRequests_.erase(it);   // superseded by a committed change
                continue;
            }
            bool busy = inflight_.count(it->first) != 0;
            for (const auto& p : pendingDynamicRecords_)
                busy = busy || (p.rec.topicId == topicId && p.rec.partition == partition);
            if (!busy) out.push_back({topicId, partition, q.requester});
            ++it;
        }
        // N1 / N2: never (re-)elected by the balancer while marked.
        for (const auto& o : offlineReplicas_) out.push_back({o.topicId, o.partition, o.node});
        for (const auto& [k, e] : emptyMemoryLeader_)
            out.push_back({static_cast<int32_t>(k >> 32), static_cast<int32_t>(static_cast<uint32_t>(k)), e.requester});
    }
    [[nodiscard]] uint64_t leaderEpochBumpsForTest() const noexcept { return leaderEpochBumps_; }
    [[nodiscard]] uint64_t bumpFailoversForTest() const noexcept { return bumpFailovers_; }
    [[nodiscard]] uint64_t leaderlessHeldForTest() const noexcept { return leaderlessHeld_; }
    [[nodiscard]] std::size_t offlineReplicasForTest() const noexcept { return offlineReplicas_.size(); }
    /// This controller may decide (it leads the metadata partition AND its map
    /// reflects its whole log as of the claim).  NodeRuntimeAgent consumes an
    /// eligible-set change only when a decision could be taken (M2).
    [[nodiscard]] bool controllerReady() const noexcept { return controllerReady_(); }
    /// D17 / DR-10 review MAJOR-1: whether THIS node is the consensus leader,
    /// pushed by the control thread every cycle (NodeRuntimeAgent).  The
    /// metadata replica's own role lags a consensus step-down (the bridge acts
    /// only on a known new leader), so deciding on the replica role alone let a
    /// stepped-down / resumed-zombie controller keep reassigning partitions.
    /// Default true: harnesses without a consensus layer decide on the role.
    void setConsensusLeads(bool leads) noexcept { consensusLeads_ = leads; }
    /// Fix round 2 (N2): this node is stopping (plain stop()) — it decides
    /// nothing any more (no rebalance, no AlterIsr answers, no emit).
    void halt() noexcept {
        if (halted_) return;
        halted_ = true;
        pendingDynamicRecords_.clear();   // undecided under this leadership: the next controller re-decides
        spdlog::info("MetadataService[{}]: halted (node stopping) — no further controller decisions", selfId_);
    }
    [[nodiscard]] bool haltedForTest() const noexcept { return halted_; }
    /// M2: a preferred-leader (re-spread) pass is due — the controller just
    /// caught up, or the committed map changed since the last pass (a returned
    /// replica re-entered an ISR: it becomes a preferred-leader candidate only
    /// THEN, after the eligible-set edge already passed).  Consumed by the
    /// caller; the pass itself only moves when it strictly improves balance.
    [[nodiscard]] bool takePreferredRecheck() noexcept {
        if (!controllerReady_()) return false;
        const uint64_t v = msm_.committedVersion();
        const bool due = recheckOnCatchUp_ || v != preferredCheckedVersion_;
        recheckOnCatchUp_ = false;
        preferredCheckedVersion_ = v;
        return due;
    }
    /// The controller decides only once its map reflects its whole log as of
    /// the claim (see controllerReady_).
    [[nodiscard]] bool controllerCaughtUpForTest() const noexcept { return controllerCaughtUp_; }
    /// F-B test-only: placement / ISR decisions queued under the current claim and not yet appended.
    [[nodiscard]] std::size_t pendingDecisionsForTest() const noexcept { return pendingDynamicRecords_.size(); }

    /// AeronPartitionTransport handler: true = send `resp` to the proposer.
    static bool onAlterIsrThunk(void* self, const rune::replication::AlterIsrMsg& m,
                                rune::replication::AlterIsrResponseMsg& resp) {
        bool answer = false;
        (void)static_cast<MetadataService*>(self)->onAlterIsr(m, &resp, &answer);
        return answer;
    }
    [[nodiscard]] uint64_t alterIsrAcceptedForTest() const noexcept { return alterIsrAccepted_; }
    [[nodiscard]] uint64_t alterIsrRejectedForTest() const noexcept { return alterIsrRejected_; }

    // -----------------------------------------------------------------------
    // MD1 Task 13: Dynamic assignment — bounded batched emit.
    //
    // Enqueues records for background emission in bounded batches across
    // duty cycles.  Each duty cycle drains at most kMaxRecordsPerBatch records
    // (and at most kMaxBatchBytes bytes), calling replica_->append() once per
    // record (the one-record-per-append invariant).
    //
    // A mass rebalance (e.g. 1000 reassignments) is split across duty cycles —
    // NEVER emitted as a single giant atomic batch.
    //
    // Called by the controller on the duty-cycle thread (single-writer).
    // The records vector is moved into the pending queue; the caller must not
    // use recs after this call returns.
    // -----------------------------------------------------------------------

    /// Fix round 2 N3 (Kafka leader.imbalance.per.broker.percentage): a
    /// preferred-leader move back to a node happens only when more than this
    /// share of the partitions it prefers are led elsewhere — a COUNT
    /// threshold, never a timer.
    static constexpr int kLeaderImbalancePct = 10;

    /// Maximum records per duty-cycle batch.
    static constexpr std::size_t kMaxRecordsPerBatch = 256;
    /// Maximum byte budget per duty-cycle batch (8 MiB).
    static constexpr std::size_t kMaxBatchBytes = 8u * 1024u * 1024u;

    /// Enqueue dynamic assignment records for bounded batch emission.
    ///
    /// @param recs  BalancerRecords to enqueue.  Ownership of each record's
    ///              owned isr/replicas vectors is transferred to the queue.
    ///              (BalancerRecord is move-only.)
    void emitDynamicAssignment(std::vector<rune::metadata::BalancerRecord>& recs) {
        // Move each record from recs into the pending queue.
        for (auto& br : recs) {
            pendingDynamicRecords_.push_back(std::move(br));
        }
        recs.clear();
    }

    // -----------------------------------------------------------------------
    // Task 14: Deterministic control-plane tick wiring.
    //
    // The LEADER owns a ControlPlaneTimer.  On each control-plane poll it checks
    // the timer against CLUSTER/LOG time; when a timer fires it APPENDS a
    // ControlTickRecord to the metadata log (monotonic tickSeq) so EVERY replica —
    // leader, follower, recovering — observes the SAME tick from the SAME log
    // entry.  The reassignment DECISION (DynamicBalancer/FailoverElection) is a
    // pure deterministic function of the committed AssignmentMap + eligibleNodes +
    // ReplicaPositionTable, so it replays identically on every node that applies
    // the tick boundary.  Mirrors Aeron Cluster's timer-tick: no replica uses a
    // local wall-clock for control decisions.
    //
    // Single-writer: must be called on the control-plane / duty-cycle thread.
    // -----------------------------------------------------------------------

    /// Arm a control-plane timer to fire at (or after) deadlineNs (cluster time).
    void armControlTick(int64_t deadlineNs, int32_t timerId = 0) {
        controlTimer_.arm(deadlineNs, timerId);
    }

    /// Leader-only: poll the control-plane timer against clusterNowNs.  If a
    /// timer is due, APPEND one ControlTickRecord (with the next monotonic
    /// tickSeq) to the metadata log and return true.  Returns false if not the
    /// leader or no timer is due.  At most ONE tick is appended per call (the
    /// earliest-due timer) — callers loop or re-arm as needed.
    ///
    /// The appended tick becomes visible (committed) on all replicas once HW
    /// advances past it; the balancer decision is then driven by
    /// runDynamicRebalanceOnTick() on whichever node is the leader.
    bool pollAndAppendControlTick(int64_t clusterNowNs) {
        // Review M4 (fix on sight): the same gate as drainDynamicQueue - a consensus-deposed or halted controller whose
        // replica still leads appends nothing.
        if (!leaderReady_() || !consensusLeads_ || halted_) return false;

        int32_t firedId = 0;
        if (!controlTimer_.poll(clusterNowNs, firedId)) return false;

        rune::protocol::metadata::ControlTickRecord tick;
        tick.tickTimeNs = clusterNowNs;
        tick.tickSeq    = nextTickSeq_++;   // monotonic across emitted ticks

        uint8_t buf[rune::protocol::metadata::ControlTickRecord::ENCODED_SIZE];
        const std::size_t n = tick.encode(buf, sizeof(buf));
        if (n == 0) return false;
        if (replica_->append(std::span<const uint8_t>(buf, n), /*acksAll=*/false) < 0) {
            // Refused (no longer the leader): the tick is not consumed —
            // re-arm it and reuse its sequence number.
            --nextTickSeq_;
            controlTimer_.arm(clusterNowNs, firedId);
            return false;
        }
        lastEmittedTickSeq_ = tick.tickSeq;
        return true;
    }

    /// Leader-only: run the delta rebalance (failover + preferred-leader +
    /// co-partition unit move) over the committed AssignmentMap using the
    /// eligibleNodes() live set (Unfenced ∧ caughtUp ∧ ¬expired — NEVER raw
    /// liveNodes()) and the soft ReplicaPositionTable, then enqueue the resulting
    /// BalancerRecords via emitDynamicAssignment() (bounded batched emit).
    ///
    /// This is the control-plane action taken WHEN a control tick fires: the tick
    /// marks the deterministic boundary; the leader computes and emits the
    /// reassignment.  Followers do NOT emit — they apply the emitted records from
    /// the committed log (the deterministic-apply guarantee).
    ///
    /// @returns the number of BalancerRecords enqueued (0 = steady state / no-op).
    std::size_t runDynamicRebalanceOnTick(
        const std::vector<rune::metadata::NodeId>& eligibleNodes,
        rune::metadata::ReplicaPositionTable*      positions,
        int maxPreferredElectionsPerCycle = 1000,
        int maxRecordsPerCycle            = 4096)
    {
        if (!controllerReady_()) return 0;

        // MEDIUM-2: the eligible set onAlterIsr filters additions against
        // (a stopping node is never admitted either — checked live there).
        const std::vector<int32_t>& shutting = shuttingNodes_();
        eligible_.assign(eligibleNodes.begin(), eligibleNodes.end());
        const bool firstLiveSet = !eligibleKnown_;
        eligibleKnown_ = true;
        if (firstLiveSet && !shutting.empty())   // a pending COMPLETE waited for the live set: an event, not a re-send
            for (auto& [n, st] : nodeLifecycle_) maybePushComplete_(n, st);
        revokeDeadLineageIdentities_();   // R1 (round 5)

        // MEDIUM-B: coordinator partitions (controller-ISR path).
        const std::size_t coord = reconcileCoordinatorPartitions(eligibleNodes);
        // Task 11 fix round 5 (I1): every member holds the broadcast "cannot serve"
        // reports; a controller PROCESS younger than one report TTL may lack one —
        // it decides no user-partition leadership until it has held them a full TTL.
        if (unservableWarmingUp()) {
            if (warmupLogged_++ == 0)
                spdlog::info("MetadataService[{}]: user-partition leader elections deferred until this process has "
                             "held the replicas' 'cannot serve' reports for {} ms", selfId_,
                             unservableWarmupNs_ / 1'000'000);
            return coord;
        }
        if (configuredTopics_.empty()) {
            // Coordinator partitions only: their bump requests (MAJOR-1).
            std::vector<rune::metadata::DynamicBalancerInputs::LeaderExclusion> ex;
            validBumpExclusions_(ex);
            std::vector<rune::metadata::BalancerRecord> recs;
            decideBumps_(recs, maxRecordsPerCycle, ex);
            decideLeaderless_(recs, maxRecordsPerCycle);
            // Fencing (dead + controlled-shutdown nodes) of the coordinator
            // ISRs — review D3: else a stopping node never leaves them.
            fenceIsrs_(eligibleNodes, recs);
            const std::size_t n = recs.size();
            if (n > 0) emitDynamicAssignment(recs);
            return coord + n;
        }

        rune::metadata::DynamicBalancerInputs in;
        in.cur       = &msm_.committedMap();
        in.liveNodes = eligibleNodes;   // eligibleNodes(), not raw liveNodes()
        in.topics    = configuredTopics_;
        in.positions = positions;
        in.maxPreferredElectionsPerCycle = maxPreferredElectionsPerCycle;
        in.maxRecordsPerCycle            = maxRecordsPerCycle;
        in.minIsr                        = minIsr_;   // commit-quorum failover floor
        in.imbalancePct                  = kLeaderImbalancePct;   // N3
        in.allowSurvivorElection         = allowSurvivorElection_;

        // DATA-PLANE LIVENESS: mark every leader whose partition frontier ADVANCED
        // since the last cycle as alive.  The frontier = max reported LEO across the
        // partition's replicas (from `positions`); only a live, serving leader can
        // drive it forward (a follower's LEO advances only by replicating from a
        // live leader).  Such a leader is never failed over even if its consensus
        // heartbeat lapsed under produce load — the acks=all leadership-churn fix.
        // First observation seeds a baseline AND grants benefit-of-the-doubt (mark
        // alive) so we never fail over before a baseline exists.  A crashed leader's
        // frontier freezes → not marked next cycle → real failover proceeds (≤1
        // cycle delay).  Control-plane duty cycle; scratch reused across calls.
        dataPlaneAliveScratch_.clear();
        if (positions != nullptr) {
            in.cur->forEach([&](int32_t topicId, int32_t partition,
                                const rune::metadata::PartitionAssignment& pa) {
                if (pa.leaderNode == rune::metadata::NO_LEADER) return;
                int64_t frontier = -1;
                for (int32_t node : pa.replicas) {
                    auto rep = positions->get(topicId, partition, node);
                    if (rep && rep->logEndPosition > frontier)
                        frontier = rep->logEndPosition;
                }
                if (frontier < 0) return;  // no positions yet → leave to heartbeat
                const int64_t k = (static_cast<int64_t>(topicId) << 32)
                                | static_cast<uint32_t>(partition);
                auto it = lastFrontier_.find(k);
                const bool advanced =
                    (it == lastFrontier_.end()) || (frontier > it->second);
                lastFrontier_[k] = frontier;
                if (advanced) dataPlaneAliveScratch_.push_back(pa.leaderNode);
            });
        }
        in.dataPlaneAliveLeaders = dataPlaneAliveScratch_;
        validBumpExclusions_(in.excludedLeaders);   // MAJOR-1
        unservableExclusions_(in.electionExcluded);  // Task 11 fix round 4 (election only)
        // Controlled shutdown: stopping nodes are AVOIDED as leaders (soft —
        // see DynamicBalancerInputs::avoidLeaders): the partitions they lead
        // move to a live in-sync replica, a sole-ISR one stays, a dead
        // leader's partition prefers a non-stopping survivor (review D5).
        in.avoidLeaders = shutting;

        rune::metadata::DynamicBalancer bal;
        std::vector<rune::metadata::BalancerRecord> recs = bal.rebalance(in);
        electionExcludedScratch_.swap(in.electionExcluded);   // M2: capacity kept for the next tick
        if (userDecisionHoldNs_ > 0 && lastNowNs_ < readySinceNs_ + userDecisionHoldNs_) [[unlikely]] {
            // TEST-ONLY hold (NodeConfig::testUserDecisionHoldMs): no USER partition decision yet.
            std::erase_if(recs, [](const rune::metadata::BalancerRecord& br) { return br.rec.topicId >= 0; });
            bumpRequests_.clear();   // re-sent by their requesters; decided after the hold
        }
        // A partition with a record in flight (e.g. a committed-ISR change from
        // AlterIsr) is decided again on the next tick, on the committed state —
        // never on a stale ISR.
        if (!inflight_.empty()) {
            std::erase_if(recs, [this](const rune::metadata::BalancerRecord& br) {
                return inflight_.count(partitionKey_(br.rec.topicId, br.rec.partition)) != 0;
            });
        }
        // D15b l1: nor on a key whose record is still QUEUED (not appended yet)
        // — e.g. the claim restored more leaderless marks than one drain emits:
        // a decision now would repeat the queued record's leaderEpoch (a wasted
        // record the MSM rejects).  Decided next tick, on the committed state.
        // Control thread, cold (a non-empty queue); scratch capacity persists.
        if (!pendingDynamicRecords_.empty() && !recs.empty()) {
            pendingKeysScratch_.clear();
            for (const auto& q : pendingDynamicRecords_)
                pendingKeysScratch_.push_back(partitionKey_(q.rec.topicId, q.rec.partition));
            std::sort(pendingKeysScratch_.begin(), pendingKeysScratch_.end());
            std::erase_if(recs, [this](const rune::metadata::BalancerRecord& br) {
                return std::binary_search(pendingKeysScratch_.begin(), pendingKeysScratch_.end(),
                                          partitionKey_(br.rec.topicId, br.rec.partition));
            });
        }
        // D15 m3: the LEADERLESS members of a co-partition unit with an
        // empty-memory member are decided by decideLeaderless_ (on fresh
        // reports) — never piecemeal here.  Siblings that HAVE a leader keep
        // the balancer's normal failover (re-review MAJOR: a held member must
        // never block a sibling whose leader died).
        if (!emptyMemoryLeader_.empty()) {
            unitScratch_.clear();
            for (const auto& [k, e] : emptyMemoryLeader_) {
                unitKeys_(k, unitVisited_);
                unitScratch_.insert(unitScratch_.end(), unitVisited_.begin(), unitVisited_.end());
            }
            std::erase_if(recs, [this](const rune::metadata::BalancerRecord& br) {
                const int64_t k = partitionKey_(br.rec.topicId, br.rec.partition);
                if (br.rec.leaderNode == rune::metadata::NO_LEADER ||
                    std::find(unitScratch_.begin(), unitScratch_.end(), k) == unitScratch_.end())
                    return false;
                const auto* cur = msm_.assignment(br.rec.topicId, br.rec.partition);
                return cur != nullptr && cur->leaderNode == rune::metadata::NO_LEADER;
            });
        }
        // MAJOR-1: the bump requests the balancer did not fail over (before
        // the fencing pass: a partition with a record is skipped there).
        decideBumps_(recs, maxRecordsPerCycle, in.excludedLeaders);
        decideLeaderless_(recs, maxRecordsPerCycle);   // N2
        fenceIsrs_(eligibleNodes, recs);
        const std::size_t count = recs.size();
        if (count > 0) emitDynamicAssignment(recs);
        return count + coord;
    }

    // -----------------------------------------------------------------------
    // GR-7 / Task-14: Partition-leadership FAILOVER + preferred-leader RESTORE.
    //
    // Called every duty cycle by NodeRuntimeAgent with the FULL, STABLE cluster
    // membership (Election::allMemberIds()) as the candidate pool — NOT a
    // heartbeat-timed liveness set.  Leadership health is TIME-INDEPENDENT: it is
    // decided by ISR (the data-plane in-sync set), so heartbeat jitter can never
    // shrink the pool or collapse leadership.  Leader-only, self-gated inside
    // runDynamicRebalanceOnTick():
    //
    //   A partition's leader is "dead" only when it is NOT in its own ISR (genuine,
    //   sustained data failure — see DynamicBalancer leaderDead).  For such a
    //   partition, DynamicBalancer runs FailoverElection over the partition's ISR
    //   and elects a surviving in-ISR replica; otherwise it may run a preferred-
    //   leader election (move to replicas[0] when it is in ISR and improves
    //   balance) to RESTORE the balanced round-robin distribution.  A healthy
    //   cluster at the balanced assignment emits nothing (idempotent, no churn).
    //
    // This is what makes a new cluster leader actually take over the partitions
    // owned by a killed node.  Without it, the committed assignment map keeps
    // naming the dead node forever and the survivor's replica stays Follower.
    //
    // Uses the internal ReplicaPositionTable (positions_); when empty (no
    // position reports wired), FailoverElection falls back to the lowest-nodeId
    // in-ISR survivor — safe here because RF == cluster size and every survivor
    // holds the full committed log.
    //
    // Idempotent: once a partition's committed leader is a live node, subsequent
    // calls emit nothing (DynamicBalancer returns 0 for that partition).
    //
    // \returns number of reassignment records emitted this call (0 = steady state).
    // Single-writer: duty-cycle thread only.
    // -----------------------------------------------------------------------
    /// Ingest a soft replica-position report into the controller's position table.
    /// Called on the control/duty thread (NodeRuntimeAgent) each cycle from the
    /// worker-published, race-free controller position snapshots.  Latest-wins per
    /// (topicId, partition, nodeId).  This is what activates the DynamicBalancer's
    /// position-gated leadership restoration after a transient-liveness ISR collapse.
    void reportReplicaPosition(int32_t topicId, int32_t partition, int32_t nodeId,
                               int32_t leaderEpoch, int64_t logEndPosition)
    {
        positions_.report(rune::replication::ReplicaPositionReport{
            topicId, partition, nodeId, leaderEpoch, logEndPosition});
    }

    std::size_t reassignDeadPartitionLeaders(
        const std::vector<rune::metadata::NodeId>& eligibleNodes,
        int maxPreferredElectionsPerCycle = 1000)
    {
        if (!controllerReady_()) return 0;
        if (eligibleNodes.empty()) return 0;
        return runDynamicRebalanceOnTick(eligibleNodes, &positions_,
                                         maxPreferredElectionsPerCycle);
    }

    /// Diagnostics: the tickSeq of the most-recently appended ControlTickRecord
    /// (-1 if none appended yet).  Monotonic across emitted ticks.
    [[nodiscard]] int64_t lastEmittedTickSeq() const noexcept { return lastEmittedTickSeq_; }

    /// Diagnostics: number of pending timers armed on the control-plane timer.
    [[nodiscard]] std::size_t pendingControlTimers() const noexcept {
        return controlTimer_.size();
    }

    // -----------------------------------------------------------------------
    // Duty-cycle entry point (single-writer thread)
    // -----------------------------------------------------------------------

    /// Apply committed records from the metadata log.
    ///
    /// On every cycle:
    ///   1. Read highWatermark() from the replica.
    ///   2. Read new batches in (lastApplied_, hw] from the log.
    ///   3. For each record in those batches, call msm_.apply().
    ///   4. Call msm_.commit(hw) to move pending→committed.
    ///   5. If leader: drain pendingDynamicRecords_ (bounded batch).
    ///
    /// On leader-transition edge (wasLeader_ gate):
    ///   - calls rebuildFromLog() to rebuild from the committed tail.
    ///
    /// \returns non-zero if any records were applied this cycle.
    int doWork(int64_t nowNs) {
        if (!replica_) return 0;
        lastNowNs_ = nowNs;
        if (firstPassNs_ < 0) firstPassNs_ = nowNs;   // I1 (round 5): report warm-up start
        if (userDecisionHoldNs_ > 0) {   // TEST-ONLY (NodeConfig::testUserDecisionHoldMs)
            const bool ready = controllerReady_();
            if (ready && !holdWasReady_) readySinceNs_ = nowNs;
            holdWasReady_ = ready;
        }

        // The log's existing content changed (a KIP-279 truncation while
        // following): everything applied from the old content is stale — rebuild
        // the MSM from the committed log as it is now.
        if (replica_->logGeneration() != lastLogGen_) {
            lastLogGen_ = replica_->logGeneration();
            // DR-10 review LOW-1: committed records are never truncated (the
            // internal-ISR candidacy gate elects only a leader holding them) —
            // a cut below what this MSM committed means that invariant broke
            // (unclean consensus election, or a bug).  The demoted follower's
            // HW reads 0, so the replica's own "below committed" warning cannot
            // fire: this is the only place that knows.  Fail-stop like every
            // other storage invariant violation — never publish a shorter map
            // (a restart rebuilds from the surviving log: committedEnd_ = 0).
            const rune::replication::Offset cut = replica_->lastTruncationOffset();
            if (cut < committedEnd_) [[unlikely]] {
                spdlog::critical("MetadataService[{}]: __cluster_metadata truncated at {} — below the "
                                 "committed end {} (committed metadata lost: unclean consensus "
                                 "election or an internal-ISR invariant bug); halting",
                                 selfId_, static_cast<long long>(cut),
                                 static_cast<long long>(committedEnd_));
                spdlog::default_logger()->flush();
                std::abort();
            }
            rebuildFromLog();
        }

        // Detect the leader edge (same pattern as TCS::loadable_): rebuild the
        // controller state from the log, then emit.
        // F-B: the controller edge is replica leadership + consensus leadership + not halted, at ONE replica
        // epoch.  A consensus step-down with no known successor leaves the replica Leader (the bridge acts only
        // on a known leader) and a same-node re-win is a Leader->Leader epoch bump: both are a leadership change,
        // never a continuation - every claim-time action (rebuild, claim LEO, catch-up -> placement of every
        // unassigned unit, forced removal reconcile, leaderless-mark restore) runs again on the re-claim.
        const bool amLeader = leaderReady_() && consensusLeads_ && !halted_;
        if (amLeader && wasLeader_ && replica_->epoch() != claimEpoch_) stepDownController_();   // re-claimed below
        if (amLeader && !wasLeader_) {
            // Became leader: rebuild the MSM from the committed log so we have
            // the full assignment map before serving requests.
            rebuildFromLog();
            wasLeader_  = true;
            claimEpoch_ = replica_->epoch();
            // Epoch-restart fix (Raft/KRaft: a new leader decides nothing before
            // its whole log is committed): records past the HW at the claim
            // (e.g. a predecessor's failover, not yet committed after a cold
            // restart) are part of the map this controller must decide on — a
            // decision on the shorter map re-uses their epochs (two leaders /
            // histories under one epoch) or re-emits the static epoch-0
            // assignment over them.  Every decision waits until the MSM has
            // applied the log through the claim-time LEO; event-driven (the HW
            // reaches it as the followers fetch — no timer).
            claimLeo_          = replica_->logEndOffset();
            controllerCaughtUp_ = false;
            // MD1 Task 6: the static partition assignment is emitted (if not
            // already assigned — idempotent) once caught up (maybeCatchUp_).
            // Followers never emit.
            maybeCatchUp_();
        } else if (!amLeader && wasLeader_) {
            stepDownController_();
        }

        const rune::replication::Offset hw = replica_->highWatermark();
        for (auto it = inflight_.begin(); it != inflight_.end();)
            it = (it->second <= hw) ? inflight_.erase(it) : std::next(it);
        for (auto it = lineageInflight_.begin(); it != lineageInflight_.end();)
            it = (it->second <= hw) ? lineageInflight_.erase(it) : std::next(it);
        for (auto it = removalInflight_.begin(); it != removalInflight_.end();)   // Task 8
            it = (it->second <= hw) ? removalInflight_.erase(it) : std::next(it);
        if (amLeader) maybeRaiseFeatureLevel_();   // D15 (no-op once at the supported level)
        if (amLeader) maybeConfirmSingleNodeLineages_();   // C1 (single node only)
        if (amLeader) retryUnplaceable_();                  // Task 11 fix round 1 (I1/I2)
        pruneUnservable_(nowNs);                            // Task 11 fix round 4
        if (hw <= 0 || hw <= lastApplied_) {
            // Nothing new to apply.  Still call commit() so pending records
            // already decoded in a prior partial cycle get committed.
            msm_.commit(hw - 1);
            if (amLeader) maybeCatchUp_();
            // Still drain the dynamic queue even if the log is caught up.
            reconcileRemovedTopics_();   // Task 8 (no-op when nothing was dropped)
            if (amLeader) drainDynamicQueue();
            onMapChangeForShutdown_();
            return 0;
        }

        // Read new batches [lastApplied_+1 .. hw) from the log.
        const rune::replication::Offset fetchFrom =
            (lastApplied_ < 0) ? 0 : lastApplied_;
        const int32_t kChunkBytes = 64 * 1024 * 1024; // 64 MiB per chunk

        int applied = 0;
        rune::replication::Offset cursor = fetchFrom;

        while (cursor < hw) {
            const std::vector<uint8_t> data =
                replica_->read(cursor, kChunkBytes);
            if (data.empty()) break;

            std::size_t pos = 0;
            rune::replication::Offset lastBatchEnd = cursor;

            while (pos < data.size()) {
                const uint8_t*    batchStart = data.data() + pos;
                const std::size_t remaining  = data.size() - pos;

                if (remaining < static_cast<std::size_t>(
                        rune::protocol::batch::kRecordBatchOverhead))
                    break;

                rune::protocol::RecordBatchReader reader(batchStart, remaining);
                if (!reader.ensureValid()) break;

                const int32_t tl = reader.totalLength();
                if (tl <= 0 || static_cast<std::size_t>(tl) > remaining) break;

                // Compute the log position of this batch.
                reader.forEachRecord([this, &applied](
                        std::string_view /*key*/,
                        std::string_view val,
                        int64_t logOffset,  // base + offDelta (absolute log position)
                        int64_t /*ts*/) {
                    msm_.apply(
                        reinterpret_cast<const uint8_t*>(val.data()),
                        val.size(),
                        logOffset);
                    ++applied;
                });

                const rune::replication::Offset batchNextOffset =
                    reader.baseOffset() + reader.lastOffsetDelta() + 1;
                if (batchNextOffset > lastBatchEnd)
                    lastBatchEnd = batchNextOffset;

                pos += static_cast<std::size_t>(tl);
            }

            if (lastBatchEnd <= cursor) break; // no progress
            cursor = lastBatchEnd;
        }

        // Commit all records up to hw-1 (log offsets are 0-indexed;
        // hw is the next-unwritten offset so the last committed is hw-1).
        msm_.commit(hw - 1);
        if (cursor > lastApplied_) lastApplied_ = cursor;
        if (hw > committedEnd_) committedEnd_ = hw;
        if (amLeader) maybeCatchUp_();

        // Drain the dynamic assignment queue (bounded per cycle).
        reconcileRemovedTopics_();   // Task 8 (no-op when nothing was dropped)
        if (amLeader) drainDynamicQueue();
        onMapChangeForShutdown_();

        return applied;
    }

    // -----------------------------------------------------------------------
    // Rebuild (called on restart or leader-transition edge)
    // -----------------------------------------------------------------------

    /// Replay the replica Log [0, highWatermark()) and reconstruct the MSM
    /// from scratch.  Mirrors ReplicatedTxnStateStore::rebuildFromLog().
    ///
    /// @returns true if any records were replayed (HW > 0).
    bool rebuildFromLog() {
        if (!replica_) return false;

        // MetadataTruncationRebuildRegression: a demoted follower's HW is 0
        // until its new leader answers — replaying only to it published an EMPTY
        // committed map and the bridge quarantined every replica of this node.
        // What this MSM already committed survives any truncation.
        const rune::replication::Offset hw = std::max(
            replica_->highWatermark(),
            std::min(committedEnd_, replica_->logEndOffset()));

        // Reset state.  The fresh MSM restarts its committed version at 0: fold
        // the old one into the epoch so routingVersion() never repeats a value.
        routingInputsEpoch_ += msm_.committedVersion() + 1;
        msm_ = rune::metadata::MetadataStateMachine{};
        lastApplied_ = -1;

        if (hw <= 0) return false;

        static constexpr int32_t kChunkBytes = 64 * 1024 * 1024;
        rune::replication::Offset cursor = 0;

        while (cursor < hw) {
            const std::vector<uint8_t> data =
                replica_->readUpTo(cursor, kChunkBytes, hw);
            if (data.empty()) break;

            std::size_t pos = 0;
            rune::replication::Offset lastBatchEnd = cursor;

            while (pos < data.size()) {
                const uint8_t*    batchStart = data.data() + pos;
                const std::size_t remaining  = data.size() - pos;

                if (remaining < static_cast<std::size_t>(
                        rune::protocol::batch::kRecordBatchOverhead))
                    break;

                rune::protocol::RecordBatchReader reader(batchStart, remaining);
                if (!reader.ensureValid()) break;

                const int32_t tl = reader.totalLength();
                if (tl <= 0 || static_cast<std::size_t>(tl) > remaining) break;

                reader.forEachRecord([this](
                        std::string_view /*key*/,
                        std::string_view val,
                        int64_t logOffset,  // absolute log position
                        int64_t /*ts*/) {
                    msm_.apply(
                        reinterpret_cast<const uint8_t*>(val.data()),
                        val.size(),
                        logOffset);
                });

                const rune::replication::Offset batchNextOffset =
                    reader.baseOffset() + reader.lastOffsetDelta() + 1;
                if (batchNextOffset > lastBatchEnd)
                    lastBatchEnd = batchNextOffset;

                pos += static_cast<std::size_t>(tl);
            }

            if (lastBatchEnd <= cursor) break;
            cursor = lastBatchEnd;
        }

        // Commit everything up to hw-1.
        msm_.commit(hw - 1);
        lastApplied_ = cursor;
        committedEnd_ = hw;
        return true;
    }

    // -----------------------------------------------------------------------
    // Query
    // -----------------------------------------------------------------------

    /// Return the committed assignment for (topicId, partition).
    /// Returns nullptr if unknown or not yet committed.
    [[nodiscard]]
    const rune::metadata::PartitionAssignment*
    assignment(int32_t topicId, int32_t partition) const noexcept {
        return msm_.assignment(topicId, partition);
    }

    /// Return the full committed AssignmentMap (read-only).
    [[nodiscard]]
    const rune::metadata::AssignmentMap& committedMap() const noexcept {
        return msm_.committedMap();
    }

    /// Changes (strictly increases) whenever an input of the RoutingImage changes:
    /// the committed assignment map, or the topic + node configuration the
    /// bootstrap (static) image is built from.  Equal value => the image built
    /// last time is still exact — the runtime rebuilds only on change.
    [[nodiscard]] uint64_t routingVersion() const noexcept {
        return routingInputsEpoch_ + msm_.committedVersion();
    }

    // -----------------------------------------------------------------------
    // MD1-13 Bootstrap fix: build a RoutingImage from the static assignment.
    //
    // When committedMap() is empty (metadata log not yet replicated), every
    // node can independently compute the same deterministic RoutingImage via
    // StaticBalancer::assign().  This eliminates the asymmetry window where
    // the Raft leader (which applies metadata first) has routingImage_ set
    // while followers still have null routingImage_ → redirect loop.
    //
    // Returns nullptr when:
    //   - configuredTopics_ or liveNodes_ is empty (not yet configured).
    //   - StaticBalancer throws BalancerError (misconfigured topic set).
    //
    // Safe to call on any node (Raft leader or follower): the static
    // assignment is deterministic — same output on all nodes given the same
    // topic+node configuration, which is wired during BrokerNode startup.
    //
    // Single-writer: must be called on the duty-cycle thread.
    // Control-plane path (not hot): allocates BalancerRecord + BuildEntry vectors.
    // -----------------------------------------------------------------------
    [[nodiscard]]
    std::shared_ptr<const rune::metadata::RoutingImage> buildStaticRoutingImage() const {
        if (configuredTopics_.empty() || liveNodes_.empty()) return nullptr;
        try {
            const auto records =
                rune::metadata::StaticBalancer::assign(configuredTopics_, liveNodes_);
            if (records.empty()) return nullptr;

            std::vector<rune::metadata::RoutingImage::BuildEntry> entries;
            entries.reserve(records.size());
            for (const auto& br : records) {
                const int32_t gpid =
                    rune::metadata::gpidOf(br.rec.topicId, br.rec.partition);
                if (gpid < 0) continue; // defensive: gpidOf is always >= 0 for user topics
                entries.push_back(rune::metadata::RoutingImage::BuildEntry{
                    gpid, br.rec.leaderNode, br.rec.leaderEpoch});
            }
            return rune::metadata::RoutingImage::build(entries);
        } catch (const rune::metadata::BalancerError&) {
            // Misconfigured topic set (e.g. RF > nodes).  Emit is already
            // suppressed in emitStaticAssignmentIfNeeded; same guard here.
            return nullptr;
        } catch (...) {
            return nullptr;
        }
    }

    // -----------------------------------------------------------------------
    // Test-only / diagnostic
    // -----------------------------------------------------------------------

    /// Return the raw MSM (for snapshot / delta derivation in tests).
    rune::metadata::MetadataStateMachine& msmForTest() noexcept { return msm_; }

private:
    // -----------------------------------------------------------------------
    // MSM-already-assigned check (idempotency §2)
    //
    // Returns true if the committed MSM already has an assignment for every
    // (topicId, partition) pair in configuredTopics_.  If it does, the leader
    // must not emit duplicates — the metadata log is already complete for this
    // topic set (e.g. after a restart where rebuildFromLog() replayed an
    // earlier assignment epoch).
    // -----------------------------------------------------------------------
    void dropAssigned_(std::vector<rune::metadata::BalancerRecord>& records) const {
        std::erase_if(records, [this](const rune::metadata::BalancerRecord& br) {
            return msm_.assignment(br.rec.topicId, br.rec.partition) != nullptr;
        });
    }

    bool msmAlreadyHasAllAssignments() const noexcept {
        for (const auto& ts : configuredTopics_) {
            for (int32_t p = 0; p < ts.partitionCount; ++p) {
                if (msm_.assignment(ts.topicId, p) == nullptr) {
                    return false;
                }
            }
        }
        return !configuredTopics_.empty(); // empty → treat as "not assigned"
    }

    // -----------------------------------------------------------------------
    // drainDynamicQueue — emit at most kMaxRecordsPerBatch records per cycle.
    //
    // Called by doWork() on the leader path only.  Drains from the front of
    // pendingDynamicRecords_, encoding each record into a stack buffer and
    // calling replica_->append() exactly once per record (one-record-per-append
    // invariant).  Stops when the count OR byte budget for this cycle is
    // exhausted, leaving the remainder in the queue for the next cycle.
    // -----------------------------------------------------------------------
    /// The controller-leadership step-down edge (replica demoted, consensus lost, halted, or a new replica
    /// epoch about to be re-claimed - F-B): everything decided under the old claim is dropped and the next
    /// claim starts a fresh catch-up.
    void stepDownController_() {
        wasLeader_ = false;
        assigned_  = false;  // re-arm emit if we regain leadership later
        // Records decided under this leadership but not appended: the next
        // leader re-decides from the committed map.
        pendingDynamicRecords_.clear();
        inflight_.clear();
        pendingLineageRecords_.clear();   // C1: the next controller re-decides (leaders re-send)
        lineageInflight_.clear();
        pendingRemovalRecords_.clear();   // Task 8: removedTopics_ is kept — the next controller removes
        removalInflight_.clear();
        removedReconciledVersion_ = UINT64_MAX;
        pendingFeatureLevel_  = -1;   // D15: the next controller re-decides
        featureInflightLevel_ = -1;
        // Controlled-shutdown marks are kept (soft, every member holds
        // them); confirmations were the old controller's — re-collected.
        for (auto& [n, st] : nodeLifecycle_) { st.confirmed.clear(); st.pushedBits = -1; st.completePushed = false; }
        confirmCheckedVersion_ = 0;
        // Follower: reset lastApplied_ so we re-apply from scratch if we
        // become leader again later.  Review M1: only when the replica really
        // stopped leading - a consensus-only deposition (or halt) keeps the log
        // as it is, so the live MSM keeps applying incrementally (re-applying the
        // whole log onto it re-bumped the committed version on every flap); a
        // re-claim's rebuildFromLog resets the cursor anyway.
        if (!replica_->acceptsProduce()) lastApplied_ = -1;
        controllerCaughtUp_ = false;   // F-B: a later claim always runs a fresh catch-up
    }

    /// This replica may append to the metadata log (the replica's produce gate).
    [[nodiscard]] bool leaderReady_() const noexcept {
        return replica_ != nullptr && replica_->acceptsProduce();
    }
    /// This replica may DECIDE (emit assignments, answer AlterIsr, rebalance):
    /// it leads AND its MSM reflects its whole log as of the claim.
    [[nodiscard]] bool controllerReady_() const noexcept {
        // Review M5: wasLeader_ = a claim edge ran (rebuild + claim LEO) - before it, nothing is decided on an empty MSM.
        return wasLeader_ && leaderReady_() && consensusLeads_ && controllerCaughtUp_ && !halted_;
    }
    /// The catch-up edge: the HW (and the MSM apply cursor) reached the
    /// claim-time LEO — the map is complete; the deferred claim action runs.
    void maybeCatchUp_() {
        if (controllerCaughtUp_ || !leaderReady_()) return;
        const rune::replication::Offset applied = lastApplied_ < 0 ? 0 : lastApplied_;   // -1 = nothing yet
        if (replica_->highWatermark() < claimLeo_ || applied < claimLeo_) return;
        controllerCaughtUp_ = true;
        recheckOnCatchUp_   = true;   // M2: re-spread decided on the complete map
        // D15: a brand-new metadata log (nothing written by any binary yet) is
        // bootstrapped at this binary's level (KRaft: metadata.version is set
        // when the log is formatted), before any assignment.
        if (claimLeo_ == 0 && msm_.featureLevel() < supportedFeatureLevel_)
            pendingFeatureLevel_ = supportedFeatureLevel_;
        restoreLeaderlessMarks_();   // D15 item 1
        emitStaticAssignmentIfNeeded();
        reconcileRemovedTopics_(/*force=*/true);   // Task 8: removals recorded while not the ready controller
        drainDynamicQueue();
    }

    /// D15: raise the committed feature level to the highest one EVERY
    /// cluster member advertised (never past this binary's).  Evaluated each
    /// leader cycle — an event (a member's registration) makes it true; no
    /// timer.  One record in flight at a time.
    void maybeRaiseFeatureLevel_() {
        const int16_t cur = msm_.featureLevel();
        if (cur >= supportedFeatureLevel_ || !controllerReady_() || clusterMembers_.empty()) return;
        if (pendingFeatureLevel_ > cur) return;                                        // queued
        if (featureInflightLevel_ > cur) return;   // appended, not applied yet (a leader never truncates it)
        int16_t target = supportedFeatureLevel_;
        for (int32_t n : clusterMembers_) {
            if (n == selfId_) continue;
            const auto it = nodeLifecycle_.find(n);
            const int16_t l = it == nodeLifecycle_.end() ? int16_t{-1} : it->second.featureLevel;
            if (l < target) target = l;
        }
        if (target <= cur) return;   // some member (an older binary, or not heard from yet) holds it
        spdlog::info("MetadataService[{}]: every member supports metadata feature level {} — raising it "
                     "from {}", selfId_, target, cur);
        pendingFeatureLevel_ = target;
        drainDynamicQueue();
    }

    /// D15 item 1: the leaderless decisions a PREDECESSOR controller took live
    /// on in the committed map (their NO_LEADER records carry a mark) — this
    /// controller re-derives its exclusions from them at the claim, and
    /// re-arms each such partition at a new leaderless epoch so every replica
    /// reports to THIS controller again (the reports the predecessor
    /// collected — and answered — died with it).  One record per marked
    /// partition per controller change; deterministic from replicated state.
    /// D15b l5 (ruling): a NO_LEADER record written by a pre-D15 controller
    /// carries no mark — its reason died with that controller.  It is left to
    /// the balancer on the committed ISR, exactly as on main (never re-armed,
    /// no mark invented): re-deciding every unmarked leaderless partition on
    /// fresh reports would hold one whose ISR member trails a non-ISR replica,
    /// where Kafka (and main) elect the ISR member.
    void restoreLeaderlessMarks_() {
        namespace rm = rune::metadata;
        msm_.committedMap().forEach([&](int32_t topicId, int32_t partition, const rm::PartitionAssignment& pa) {
            if (pa.leaderNode != rm::NO_LEADER || pa.mark.kind == rm::LeaderlessMark::Kind::None) return;
            const int64_t key = partitionKey_(topicId, partition);
            if (pa.mark.kind == rm::LeaderlessMark::Kind::LogDirOffline)
                markOffline_(topicId, partition, pa.mark.node, pa.mark.incarnation);
            rm::BalancerRecord br;
            br.isr      = pa.isr;
            br.replicas = pa.replicas;
            br.mark     = pa.mark;
            br.rec.topicId        = topicId;
            br.rec.partition      = partition;
            br.rec.leaderNode     = rm::NO_LEADER;
            br.rec.leaderEpoch    = pa.leaderEpoch + 1;
            br.rec.partitionEpoch = std::max(pa.partitionEpoch + 1, br.rec.leaderEpoch);
            br.rec.isr      = rune::protocol::metadata::Int32ArrayView{br.isr.data(), static_cast<uint16_t>(br.isr.size())};
            br.rec.replicas = rune::protocol::metadata::Int32ArrayView{br.replicas.data(), static_cast<uint16_t>(br.replicas.size())};
            if (pa.mark.kind == rm::LeaderlessMark::Kind::EmptyMemoryLeader) {
                emptyMemoryLeader_[key] = EmptyMemoryLeader{pa.mark.node, br.rec.leaderEpoch, pa.mark.incarnation};
                leaderlessReports_.erase(key);
            }
            ++leaderlessMarksRestored_;
            pendingDynamicRecords_.push_back(std::move(br));
        });
        // Review m3: the unit's OTHER leaderless members are decided with it —
        // they report to this controller too (re-armed, no mark).
        std::vector<int64_t> rearmed, unit;   // cold (a claim)
        for (const auto& q : pendingDynamicRecords_) rearmed.push_back(partitionKey_(q.rec.topicId, q.rec.partition));
        for (const auto& [k, e] : emptyMemoryLeader_) {
            unitKeys_(k, unit);
            for (int64_t u : unit) {
                if (std::find(rearmed.begin(), rearmed.end(), u) != rearmed.end()) continue;
                const auto* pa = msm_.assignment(static_cast<int32_t>(u >> 32), static_cast<int32_t>(static_cast<uint32_t>(u)));
                if (pa == nullptr || pa->leaderNode != rm::NO_LEADER) continue;
                rm::BalancerRecord br;
                br.isr      = pa->isr;
                br.replicas = pa->replicas;
                br.rec.topicId        = static_cast<int32_t>(u >> 32);
                br.rec.partition      = static_cast<int32_t>(static_cast<uint32_t>(u));
                br.rec.leaderNode     = rm::NO_LEADER;
                br.rec.leaderEpoch    = pa->leaderEpoch + 1;
                br.rec.partitionEpoch = std::max(pa->partitionEpoch + 1, br.rec.leaderEpoch);
                br.rec.isr      = rune::protocol::metadata::Int32ArrayView{br.isr.data(), static_cast<uint16_t>(br.isr.size())};
                br.rec.replicas = rune::protocol::metadata::Int32ArrayView{br.replicas.data(), static_cast<uint16_t>(br.replicas.size())};
                rearmed.push_back(u);
                pendingDynamicRecords_.push_back(std::move(br));
            }
        }
    }

    /// Task 8: on the READY controller, queue ONE RemoveTopicPlacementRecord for
    /// every dropped topic the committed map still places (and no removal of it
    /// queued / in flight).  removedTopics_ is never forgotten in this process:
    /// a placement of a dropped topic can still COMMIT later (an assignment
    /// appended before the drop, by this or a predecessor controller) and must
    /// be removed then too — so every broker keeps the set, whichever becomes
    /// controller completes it.  Runs only when something changed (the
    /// committed map's version, a new drop, the readiness edge): cold.
    /// rf 0 = "every broker" → every PLACEABLE broker, resolved now (review M3:
    /// logged when a catalog-halted broker makes it smaller than the cluster —
    /// the topic keeps that replica count after the broker recovers).
    void resolveDefaultRf_(rune::metadata::TopicSpec& spec) const {
        const std::size_t placeable = placementNodes().size();
        spec.rf = static_cast<int16_t>(std::max<std::size_t>(1, placeable));
        if (!liveNodes_.empty() && placeable < liveNodes_.size())
            spdlog::warn("MetadataService[{}]: topic id={} (replicationFactor = every broker) is placed on {} of {} "
                         "brokers — catalog-halted brokers are never placement targets; it keeps {} replicas",
                         selfId_, spec.topicId, spec.rf, liveNodes_.size(), spec.rf);
    }
    /// The configured topics of `t`'s placement unit (its co-partition group, or itself).
    void unitOf_(const rune::metadata::TopicSpec& t, std::vector<rune::metadata::TopicSpec>& out) const {
        out.clear();
        if (t.coPartitionGroup.empty()) { out.push_back(t); return; }
        for (const auto& ts : configuredTopics_) if (ts.coPartitionGroup == t.coPartitionGroup) out.push_back(ts);
    }
    [[nodiscard]] bool unitFullyAssigned_(const std::vector<rune::metadata::TopicSpec>& unit) const noexcept {
        for (const auto& ts : unit)
            for (int32_t p = 0; p < ts.partitionCount; ++p)
                if (msm_.assignment(ts.topicId, p) == nullptr) return false;
        return true;
    }
    /// Place one unit (controller): records enqueued (not drained), or the unit
    /// held Unplaceable.  Returns false only when the unit is unplaceable.
    bool placeUnit_(const std::vector<rune::metadata::TopicSpec>& unit) {
        if (unit.empty()) return true;
        if (unitFullyAssigned_(unit)) {
            for (const auto& ts : unit) unplaceable_.erase(ts.topicId);
            return true;
        }
        std::vector<rune::metadata::BalancerRecord> records;
        try {
            records = rune::metadata::StaticBalancer::assign(unit, placementNodes());
        } catch (const rune::metadata::BalancerError& ex) {
            ++balancerErrors_;
            std::string halted;
            for (int32_t n : liveNodes_)
                if (catalogHalted(n)) halted += (halted.empty() ? "" : ",") + std::to_string(n);
            const std::string reason = std::string(ex.what()) + " (placeable brokers " +
                                       std::to_string(placementNodes().size()) +
                                       (halted.empty() ? std::string{} : "; catalog apply halted on broker(s) " + halted) +
                                       ")";
            for (const auto& ts : unit) {
                auto [it, inserted] = unplaceable_.try_emplace(ts.topicId, UnplaceableTopic{reason});
                if (inserted || it->second.reason != reason) {
                    it->second.reason = reason;
                    ++unplaceableEvents_;
                    spdlog::error("MetadataService[{}]: topic id={} is UNPLACEABLE — {}; it serves nowhere until the "
                                  "placeable brokers change (retried then; rune-catalog-unplaceable-topics)",
                                  selfId_, ts.topicId, reason);
                }
            }
            return false;
        }
        dropAssigned_(records);   // never overwrite an existing assignment
        for (auto& br : records) pendingDynamicRecords_.push_back(std::move(br));
        for (const auto& ts : unit) {
            if (unplaceable_.erase(ts.topicId) != 0)
                spdlog::info("MetadataService[{}]: topic id={} is placeable again — placed", selfId_, ts.topicId);
        }
        return true;
    }
    /// Every configured unit with an unassigned partition, each on its own.
    void placeEveryUnassignedUnit_() {
        if (placementHeldForTest_) [[unlikely]] return;   // Task 22 seam covers EVERY placement path (M8)
        std::vector<rune::metadata::TopicSpec> unit;
        std::vector<std::string> groupsDone;
        for (const auto& ts : configuredTopics_) {
            if (!ts.coPartitionGroup.empty()) {
                if (std::find(groupsDone.begin(), groupsDone.end(), ts.coPartitionGroup) != groupsDone.end()) continue;
                groupsDone.push_back(ts.coPartitionGroup);
            }
            unitOf_(ts, unit);
            (void)placeUnit_(unit);
        }
    }
    /// Controller: re-try the Unplaceable units once per placement-input change.
    void retryUnplaceable_() {
        if (unplaceable_.empty() || routingInputsEpoch_ == unplaceableTriedEpoch_) return;
        if (!controllerReady_() || catalogHalted(selfId_) || placementHeldForTest_) return;   // seam: M8
        unplaceableTriedEpoch_ = routingInputsEpoch_;
        // Forget topics no longer configured (dropped).
        for (auto it = unplaceable_.begin(); it != unplaceable_.end();) {
            bool configured = false;
            for (const auto& ts : configuredTopics_) configured = configured || ts.topicId == it->first;
            it = configured ? std::next(it) : unplaceable_.erase(it);
        }
        std::vector<int32_t> ids;
        for (const auto& [id, u] : unplaceable_) ids.push_back(id);
        std::vector<rune::metadata::TopicSpec> unit;
        for (int32_t id : ids) {
            if (!unplaceable_.contains(id)) continue;   // placed with an earlier unit
            for (const auto& ts : configuredTopics_)
                if (ts.topicId == id) { unitOf_(ts, unit); (void)placeUnit_(unit); break; }
        }
        drainDynamicQueue();
    }

    void noteUnservable_(const rune::replication::AlterIsrMsg& m) {
        const int64_t now = lastNowNs_;
        // Review M4 (round 5) / fix round 6: a late report of an OLDER (superseded)
        // process of the node is dropped — never one of a NEWER process (a restarted
        // broker's first report can arrive before its node-control register);
        // a report about a partition it is not a replica of is ignored.
        const int64_t inc = incarnationOf_(m);
        if (const auto it = nodeLifecycle_.find(m.leaderId);
            inc != 0 && it != nodeLifecycle_.end() && it->second.isSuperseded(inc))
            return;
        if (const auto* pa = msm_.assignment(m.topicId, m.partition);
            pa != nullptr && std::find(pa->replicas.begin(), pa->replicas.end(), m.leaderId) == pa->replicas.end())
            return;
        for (auto& u : unservable_)
            if (u.topicId == m.topicId && u.partition == m.partition && u.node == m.leaderId) {
                u.lastSeenNs = now;
                return;
            }
        unservable_.push_back({m.topicId, m.partition, m.leaderId, now});
        spdlog::warn("MetadataService[{}]: node {} reports it cannot serve ({}, {}) — never elected its leader while "
                     "it reports", selfId_, m.leaderId, m.topicId, m.partition);
    }
    void unservableExclusions_(std::vector<rune::metadata::DynamicBalancerInputs::LeaderExclusion>& out) {
        // Review M2 (round 5): the vector is the reused member scratch, moved in
        // and back by the tick (capacity persists — no per-tick allocation).
        out.swap(electionExcludedScratch_);
        out.clear();
        for (const auto& u : unservable_) out.push_back({u.topicId, u.partition, u.node});
    }
    /// Expire stale reports; derive the BLOCKED set (count + log).  Allocation-free
    /// when there is no report and nothing blocked.
    void pruneUnservable_(int64_t nowNs) {
        if (unservable_.empty() && blocked_.empty()) [[likely]] return;
        std::erase_if(unservable_, [&](const UnservableReport& u) {
            if (nowNs - u.lastSeenNs <= kUnservableReportTtlNs) return false;
            spdlog::info("MetadataService[{}]: node {} no longer reports ({}, {}) unservable — electable again",
                         selfId_, u.node, u.topicId, u.partition);
            return true;
        });
        blockedScratch_.clear();
        // Fix round 6 (r5 m2): every member HOLDS the reports (I1), but only the
        // ready controller derives, logs and gauges BLOCKED — never one count per member.
        if (!controllerReady_()) {
            blocked_.clear();
            return;
        }
        for (const auto& u : unservable_) {
            const rune::metadata::PartitionAssignment* pa = msm_.assignment(u.topicId, u.partition);
            if (pa == nullptr) continue;
            bool electable = false;
            for (int32_t n : pa->isr) {
                if (reportedUnservable(u.topicId, u.partition, n) || isShutting_(n)) continue;
                if (eligibleKnown_ && !isEligible_(n) && n != selfId_) continue;
                electable = true;
            }
            const int64_t k = partitionKey_(u.topicId, u.partition);
            if (!electable && std::find(blockedScratch_.begin(), blockedScratch_.end(), k) == blockedScratch_.end())
                blockedScratch_.push_back(k);
        }
        for (int64_t k : blockedScratch_)
            if (std::find(blocked_.begin(), blocked_.end(), k) == blocked_.end())
                spdlog::error("MetadataService[{}]: ({}, {}) is BLOCKED — no live in-sync replica can serve it (every "
                              "one reports it cannot); it is never led by a replica without its data and recovers "
                              "when one serves again (rune-catalog-blocked-partitions)", selfId_, k >> 32,
                              static_cast<uint32_t>(k));
        for (int64_t k : blocked_)
            if (std::find(blockedScratch_.begin(), blockedScratch_.end(), k) == blockedScratch_.end())
                spdlog::info("MetadataService[{}]: ({}, {}) is no longer blocked", selfId_, k >> 32,
                             static_cast<uint32_t>(k));
        blocked_.swap(blockedScratch_);
    }

    void reconcileRemovedTopics_(bool force = false) {
        if (removedTopics_.empty() || !controllerReady_()) return;
        const uint64_t v = msm_.committedVersion();
        if (!force && v == removedReconciledVersion_) return;
        removedReconciledVersion_ = v;
        // Task 11 fix round 0: a halted controller unplaces no catalog topic (a
        // healthy successor does: removedTopics_ is kept).
        if (catalogHalted(selfId_)) { ++catalogPlacementsSkippedHalted_; return; }
        bool queued = false;
        msm_.committedMap().forEach([&](int32_t t, int32_t, const rune::metadata::PartitionAssignment&) {
            if (!std::binary_search(removedTopics_.begin(), removedTopics_.end(), t)) return;
            if (removalInflight_.count(t) != 0) return;
            if (std::find(pendingRemovalRecords_.begin(), pendingRemovalRecords_.end(), t) !=
                pendingRemovalRecords_.end())
                return;
            pendingRemovalRecords_.push_back(t);
            queued = true;
        });
        if (queued) drainDynamicQueue();
    }

    void drainDynamicQueue() {
        if (pendingDynamicRecords_.empty() && pendingLineageRecords_.empty() && pendingRemovalRecords_.empty() &&
            pendingFeatureLevel_ <= msm_.featureLevel())
            return;
        // kept queued until this replica may append; F-B / D17: a consensus-deposed or halted controller whose
        // replica still leads never appends what it decided (the step-down edge also clears the queues).
        if (!leaderReady_() || !consensusLeads_ || halted_) return;

        static constexpr std::size_t kEncBufSize = 16 * 1024;
        uint8_t encBuf[kEncBufSize];

        std::size_t cycleRecords = 0;
        std::size_t cycleBytes   = 0;

        // C1 multi-node: transient lineage records (one per confirmed leadership).
        while (!pendingLineageRecords_.empty() && cycleRecords < kMaxRecordsPerBatch) {
            const auto& lr = pendingLineageRecords_.front();
            const std::size_t n = lr.encode(encBuf, kEncBufSize);
            if (n == 0) { pendingLineageRecords_.pop_front(); continue; }
            if (replica_->append(std::span<const uint8_t>(encBuf, n), /*acksAll=*/false) < 0) return;
            lineageInflight_[partitionKey_(lr.topicId, lr.partition)] = replica_->logEndOffset();
            pendingLineageRecords_.pop_front();
            ++cycleRecords;
            cycleBytes += n;
        }

        // Catalog history Task 8: whole-topic placement removals (dropped topics).
        while (!pendingRemovalRecords_.empty() && cycleRecords < kMaxRecordsPerBatch) {
            rune::metadata::RemoveTopicPlacementRecord rr;
            rr.topicId = pendingRemovalRecords_.front();
            const std::size_t n = rr.encode(encBuf, kEncBufSize);
            if (replica_->append(std::span<const uint8_t>(encBuf, n), /*acksAll=*/false) < 0) return;
            removalInflight_[rr.topicId] = replica_->logEndOffset();
            pendingRemovalRecords_.pop_front();
            ++cycleRecords;
            cycleBytes += n;
        }

        if (pendingFeatureLevel_ > msm_.featureLevel()) {   // D15: ahead of the assignments
            rune::metadata::FeatureLevelRecord fr;
            fr.level = pendingFeatureLevel_;
            const std::size_t n = fr.encode(encBuf, kEncBufSize);
            if (replica_->append(std::span<const uint8_t>(encBuf, n), /*acksAll=*/false) < 0) return;
            featureInflightLevel_ = pendingFeatureLevel_;
            pendingFeatureLevel_  = -1;
            ++cycleRecords;
        }

        while (!pendingDynamicRecords_.empty()) {
            if (cycleRecords >= kMaxRecordsPerBatch) break;

            const auto& br = pendingDynamicRecords_.front();
            std::size_t n = br.rec.encode(encBuf, kEncBufSize);
            if (n != 0 && br.mark.kind != rune::metadata::LeaderlessMark::Kind::None) {
                // D15: the leaderless mark rides as a trailer older MSMs ignore.
                const std::size_t t = rune::metadata::leaderless_trailer::encode(encBuf + n, kEncBufSize - n, br.mark);
                n = t == 0 ? 0 : n + t;
            }
            if (n == 0) {
                // Encode overflow — skip this record (should not occur at Phase-1 scale).
                pendingDynamicRecords_.pop_front();
                continue;
            }
            if (cycleBytes + n > kMaxBatchBytes) break;

            // Never ignore the result: a refusal (no longer the leader)
            // keeps the record queued, in order.
            if (replica_->append(std::span<const uint8_t>(encBuf, n), /*acksAll=*/false) < 0) break;
            // In flight until committed: no other decision (AlterIsr, rebalance)
            // for this partition is taken on a state this record will change.
            inflight_[partitionKey_(br.rec.topicId, br.rec.partition)] = replica_->logEndOffset();
            cycleBytes   += n;
            cycleRecords += 1;
            pendingDynamicRecords_.pop_front();
        }
    }

    rune::replication::PartitionReplica* replica_;   ///< non-owning
    int32_t                               selfId_;

    rune::metadata::MetadataStateMachine  msm_;
    uint64_t                              routingInputsEpoch_ = 0;   // see routingVersion()

    // Last log offset we have read batches from.  Initialized to -1 (nothing applied).
    rune::replication::Offset lastApplied_ = -1;
    // MetadataTruncationRebuildRegression: exclusive end of the log the MSM has
    // COMMITTED.  Committed records are never truncated (KIP-101/279 cut only
    // above the HW), so a rebuild after a truncation replays at least this far
    // (capped at the LEO) even while the freshly-demoted follower's HW reads 0.
    rune::replication::Offset committedEnd_ = 0;
    bool consensusLeads_ = true;   ///< D17 / review MAJOR-1 (setConsensusLeads)

    // Follower→Leader transition gate (mirrors TCS / GCS patterns).
    bool wasLeader_ = false;
    rune::replication::Epoch claimEpoch_ = -1;   ///< F-B: the replica epoch of the current controller claim
    /// partition key → metadata-log end offset of its last appended (not yet
    /// committed) assignment record.
    std::unordered_map<int64_t, rune::replication::Offset> inflight_;
    uint64_t alterIsrAccepted_ = 0, alterIsrRejected_ = 0;
    /// MEDIUM-2: the eligible (unfenced, live) set of the last rebalance tick —
    /// onAlterIsr never adds a node outside it.  Unknown before the first tick.
    std::vector<int32_t> eligible_;
    bool                 eligibleKnown_ = false;
    static int64_t partitionKey_(int32_t topicId, int32_t partition) noexcept {
        return (static_cast<int64_t>(topicId) << 32) | static_cast<uint32_t>(partition);
    }
    std::vector<rune::metadata::PartitionKey> coordinatorKeys_;      ///< MEDIUM-B
    std::vector<int32_t>                      coordinatorReplicas_;  ///< MEDIUM-B
    /// replica_->logGeneration() the MSM was built against (see doWork).
    uint64_t lastLogGen_ = 0;
    /// Epoch-restart fix: the metadata LEO at this controller's claim, and
    /// whether the MSM has applied the log through it (true before any claim:
    /// direct-invocation unit rigs decide on what they applied).
    rune::replication::Offset claimLeo_ = 0;
    bool     controllerCaughtUp_ = true;
    bool     halted_             = false;   ///< fix round 2 N2: stopping — decides nothing
    bool     recheckOnCatchUp_   = false;
    uint64_t preferredCheckedVersion_ = 0;
    uint64_t leaderEpochBumps_   = 0;
    uint64_t bumpFailovers_      = 0;
    /// MAJOR-1: bump requests answered by a FAILOVER (another live ISR member
    /// leads; the requester — a restarted ex-leader that may have lost acked
    /// records — rejoins as a follower).  key → the request; decided by the
    /// next rebalance tick (co-partition units move together).
    struct BumpRequest {
        int32_t requester      = -1;
        int32_t leaderEpoch    = -1;
        int32_t partitionEpoch = -1;
        int32_t fence          = -1;
        bool    memoryBacked   = false;
        bool    logDirFailed   = false;   ///< N1
        int32_t leo            = -1;      ///< the requester's LEO (clamped)
        int64_t incarnation    = 0;
    };
    std::unordered_map<int64_t, BumpRequest> bumpRequests_;
    /// Controlled shutdown: node → {its newest incarnation seen, stopping?}.
    std::unordered_map<int32_t, NodeLifecycle> nodeLifecycle_;
    std::vector<int32_t> shuttingScratch_;
    std::vector<rune::replication::AlterIsrMsg> nodeControlOut_;   ///< member confirmations to send
    std::vector<std::pair<int32_t, rune::replication::AlterIsrResponseMsg>> nodeControlRespOut_;
    uint64_t confirmCheckedVersion_ = 0;
    int32_t  knownController_       = -1;
    uint64_t controlledShutdownRequests_ = 0;
    /// Fix round 2 N1 (KIP-112): replicas whose log dir went offline — never
    /// (re-)elected for that partition until a report from a NEW incarnation.
    struct OfflineReplica { int32_t topicId; int32_t partition; int32_t node; int64_t incarnation; };
    std::vector<OfflineReplica> offlineReplicas_;
    /// N2: the last process incarnation seen from each node (bump / report);
    /// a change drops the node's (now stale) position reports.
    std::unordered_map<int32_t, int64_t> nodeIncarnation_;
    /// N2: a memory-backed sole-ISR leader restarted EMPTY — its partition was
    /// made leaderless (ISR kept) and the next leader is decided on the FRESH
    /// leaderless reports of every live replica (decideLeaderless_).
    struct EmptyMemoryLeader { int32_t requester; int32_t leaderlessEpoch; int64_t incarnation = 0; };
    std::unordered_map<int64_t, EmptyMemoryLeader> emptyMemoryLeader_;
    uint64_t leaderlessMarksRestored_ = 0;   ///< D15: marks re-derived from the committed map
    /// D15: feature level (see setClusterMembers).
    std::vector<int32_t> clusterMembers_;
    int16_t supportedFeatureLevel_ = rune::replication::MetadataFeature::kLatest;
    int16_t pendingFeatureLevel_   = -1;   ///< a FeatureLevelRecord to append (-1 none)
    int16_t featureInflightLevel_  = -1;   ///< appended under this leadership, not applied yet
    /// D15 item 2: decideLeaderless_ scratch (capacity persists).
    std::vector<int64_t> unitScratch_, unitVisited_, unitDecided_;
    std::vector<int64_t> pendingKeysScratch_;   ///< D15b l1: queued keys at a rebalance (sorted)
    struct FreshReport { int32_t leo = -1; bool logDirFailed = false; int32_t epoch = -1; };
    std::unordered_map<int64_t, std::unordered_map<int32_t, FreshReport>> leaderlessReports_;
    uint64_t leaderlessHeld_ = 0;   ///< partitions held leaderless (log dir failed / unclean needed)
    std::unordered_set<int64_t> heldLeaderless_;

    // -----------------------------------------------------------------------
    // MD1 Task 6: static assignment emit state.
    // -----------------------------------------------------------------------

    /// User topic specs for static assignment (topicId >= 0 only).
    std::vector<rune::metadata::TopicSpec>  configuredTopics_;
    /// Live node IDs for the balancer.
    std::vector<rune::metadata::NodeId>     liveNodes_;
    /// Commit-quorum failover floor handed to the DynamicBalancer (see
    /// DynamicBalancerInputs::minIsr).  Set by BrokerNode from gatewayMinIsr.
    int                                     minIsr_ = 1;
    bool                                    allowSurvivorElection_ = false;   ///< MEDIUM-3 opt-in
    /// Data-plane liveness state (duty-thread only): last-observed replication
    /// frontier per (topicId<<32|partition), used to detect whether a leader's
    /// partition is still advancing → the leader is alive despite heartbeat lapse.
    std::unordered_map<int64_t, int64_t>    lastFrontier_;
    /// Reused scratch for the per-cycle data-plane-alive leader set (capacity
    /// persists after warmup → no steady-state heap alloc on the control plane).
    std::vector<int32_t>                    dataPlaneAliveScratch_;
    /// Idempotency latch: true after the first successful emit.
    bool assigned_ = false;
    /// Error counter: incremented when BalancerError is caught.
    uint64_t balancerErrors_ = 0;

    // -----------------------------------------------------------------------
    // MD1 Task 13: dynamic assignment pending queue.
    //
    // Records submitted via emitDynamicAssignment() are enqueued here.
    // drainDynamicQueue() drains from the front across duty cycles.
    // std::deque is used so pop_front() is O(1) and push_back() doesn't
    // invalidate existing elements.
    // -----------------------------------------------------------------------
    std::deque<rune::metadata::BalancerRecord> pendingDynamicRecords_;
    // C1 multi-node: lineage decisions to append, and the appended-not-yet-
    // committed ones (partition key -> metadata-log end offset).
    std::deque<rune::metadata::TransientLineageRecord> pendingLineageRecords_;
    // Catalog history Task 8 (removeRuntimeTopic): the topics the catalog dropped
    // (every broker records them — a successor controller completes the removal;
    // kept for the process: see reconcileRemovedTopics_), the committed-map
    // version last reconciled, the removals decided but not appended, and
    // topic → metadata-log end offset of its appended, uncommitted removal.
    std::vector<int32_t>                               removedTopics_;   // sorted
    uint64_t                                           removedReconciledVersion_ = UINT64_MAX;
    std::deque<int32_t>                                pendingRemovalRecords_;
    std::unordered_map<int32_t, rune::replication::Offset> removalInflight_;
    bool    placementHeldForTest_ = false;   ///< Task 22 seam (holdPlacementForTest)
    int64_t userDecisionHoldNs_ = 0, readySinceNs_ = 0, lastNowNs_ = 0;   ///< TEST-ONLY hold
    bool    holdWasReady_       = false;
    std::unordered_map<int64_t, rune::replication::Offset> lineageInflight_;
    uint64_t lineagesMinted_ = 0, lineagesKept_ = 0, lineageMintCounter_ = 0;
    uint64_t lineageSalt_ = 0x5DEECE66Dull;
    int64_t  lineageAuthorityIncarnation_ = 0;   ///< single node only (0 = replicas report)
    bool   (*lineageIsTransient_)(void*, int32_t) = nullptr;
    void*    lineageIsTransientCtx_ = nullptr;
    uint64_t lineageScanMapVersion_ = UINT64_MAX, lineageScanLineageVersion_ = UINT64_MAX;

    // -----------------------------------------------------------------------
    // Task 14: control-plane tick state (leader-owned).
    // -----------------------------------------------------------------------
    /// Deterministic priority-heap timer polled against cluster/log time.
    rune::metadata::ControlPlaneTimer controlTimer_;
    /// Next monotonic tickSeq to stamp on the next appended ControlTickRecord.
    int64_t nextTickSeq_        = 0;
    /// tickSeq of the last appended ControlTickRecord (-1 = none yet).
    int64_t lastEmittedTickSeq_ = -1;

    /// GR-7: soft replica-position table consumed by FailoverElection when
    /// reassignDeadPartitionLeaders() runs.  Empty in the current wiring (no
    /// position reports plumbed yet) → FailoverElection deterministically picks
    /// the lowest-nodeId in-ISR survivor, which is correct when RF == cluster
    /// size (every survivor holds the full committed log).  Kept as a member so
    /// future position-report wiring can seed it without an API change.
    rune::metadata::ReplicaPositionTable positions_;
};

} // namespace rune::node
