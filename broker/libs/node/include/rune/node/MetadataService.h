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
// COORDINATOR partitions (__consumer_offsets=-1, __transaction_state=-2) are
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
#include <rune/replication/PartitionReplica.h>
#include <rune/protocol/RecordBatch.h>  // RecordBatchReader
#include <rune/protocol/MetadataRecords.h>

#include <algorithm>
#include <cstdint>
#include <deque>
#include <optional>
#include <span>
#include <unordered_map>
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
        configuredTopics_.clear();
        configuredTopics_.reserve(topics.size());
        for (const auto& ts : topics) {
            if (ts.topicId >= 0) {
                configuredTopics_.push_back(ts);
            }
        }
        liveNodes_ = liveNodes;
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
    [[nodiscard]] static constexpr bool isCoordinatorTopic(int32_t topicId) noexcept {
        return topicId == -1 || topicId == -2;
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
        const std::vector<int32_t>& replicas)
    {
        namespace rm = rune::metadata;
        auto in = [](int32_t n, const std::vector<int32_t>& v) {
            return std::find(v.begin(), v.end(), n) != v.end();
        };
        if (cur == nullptr) {
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
        if (cur->leaderNode == selfId) return std::nullopt;
        std::vector<int32_t> live = eligible;
        if (!in(selfId, live)) live.push_back(selfId);   // the controller is live
        rm::AssignmentChange chg;
        if (in(selfId, cur->isr)) {
            chg.newLeader = selfId;
            chg.newIsr.push_back(selfId);
            for (int32_t n : cur->isr) if (n != selfId) chg.newIsr.push_back(n);
        } else if (cur->leaderNode != rm::NO_LEADER && in(cur->leaderNode, live) &&
                   in(cur->leaderNode, cur->isr)) {
            return std::nullopt;
        } else {
            rm::ElectionInputs ei;
            ei.cur            = *cur;
            ei.cur.leaderNode = rm::NO_LEADER;   // dead (or none): always elect
            ei.liveNodes      = live;
            ei.positions      = positions;
            ei.allowUnclean   = false;
            const rm::ElectionOutcome out = rm::FailoverElection::elect(topicId, partition, ei);
            if (out.change.newLeader == rm::NO_LEADER_CHANGE) return std::nullopt;
            chg.newLeader = out.change.newLeader;
            if (chg.newLeader != rm::NO_LEADER) {
                chg.newIsr.push_back(chg.newLeader);
                for (int32_t n : cur->isr)
                    if (n != chg.newLeader && in(n, live)) chg.newIsr.push_back(n);
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
            const auto next = decideCoordinatorPartition(
                k.topicId, k.partition, selfId_, msm_.assignment(k.topicId, k.partition),
                eligibleNodes, &positions_, bootstrapEpoch, coordinatorReplicas_);
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
    void addRuntimeTopic(const rune::metadata::TopicSpec& spec) {
        if (spec.topicId < 0) return;  // internal topics are not assigned

        // (1) Record the topic in the configured set (idempotent by topicId) so
        // it participates in routing-image builds + future rebalances.
        bool present = false;
        for (const auto& ts : configuredTopics_) {
            if (ts.topicId == spec.topicId) { present = true; break; }
        }
        if (!present) { configuredTopics_.push_back(spec); ++routingInputsEpoch_; }

        // (2) Emit the initial assignment for this topic (leader only).
        if (!controllerReady_()) return;
        if (liveNodes_.empty()) return;

        // Per-topic idempotency: skip if the MSM already has every partition
        // assigned (e.g. re-invocation after a restart-rebuild replayed it).
        bool allAssigned = spec.partitionCount > 0;
        for (int32_t p = 0; p < spec.partitionCount; ++p) {
            if (msm_.assignment(spec.topicId, p) == nullptr) { allAssigned = false; break; }
        }
        if (allAssigned) return;

        std::vector<rune::metadata::TopicSpec> one{spec};
        std::vector<rune::metadata::BalancerRecord> records;
        try {
            records = rune::metadata::StaticBalancer::assign(one, liveNodes_);
        } catch (const rune::metadata::BalancerError&) {
            ++balancerErrors_;
            return;
        }
        if (records.empty()) return;

        // One-record-per-append through the gated queue (never an ignored
        // append result: a refused record stays queued).
        for (auto& br : records) pendingDynamicRecords_.push_back(std::move(br));
        drainDynamicQueue();
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
        if (configuredTopics_.empty() || liveNodes_.empty()) return;

        // Idempotency guard: if already assigned (flag latched), skip.
        if (assigned_) return;

        // Secondary idempotency check: if MSM already has ALL user-topic
        // partition assignments committed, skip re-emission.
        if (msmAlreadyHasAllAssignments()) {
            assigned_ = true;
            return;
        }

        // Run StaticBalancer.
        std::vector<rune::metadata::BalancerRecord> records;
        try {
            records = rune::metadata::StaticBalancer::assign(configuredTopics_, liveNodes_);
        } catch (const rune::metadata::BalancerError& ex) {
            // Balancer validation failure (e.g. RF > nodes, co-partition mismatch).
            // Increment the error counter so callers / tests can detect silent failure.
            // Do NOT crash the duty-cycle — this must not happen in production when
            // config is validated at startup.
            (void)ex;
            ++balancerErrors_;
            return;
        }

        if (records.empty()) {
            assigned_ = true;
            return;
        }

        // One-record-per-append invariant (required by the read path): each
        // metadata record occupies its own log slot (concatenating N records into
        // one payload would make replay decode only the first).  The records go
        // through the gated queue (drainDynamicQueue): appended in order while
        // this replica leads, kept queued — never dropped, never an ignored
        // append result — while an append is refused.
        for (auto& br : records) pendingDynamicRecords_.push_back(std::move(br));
        drainDynamicQueue();

        assigned_ = true;
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
        if (!controllerReady_()) return false;
        // Internal topics: only the coordinator partitions on the controller
        // path (MEDIUM-B); __cluster_metadata uses the internal-ISR store.
        if ((m.topicId < 0 && !coordinatorKey_(m.topicId, m.partition)) || m.isrCount < 0) return false;
        const rune::metadata::PartitionAssignment* pa = msm_.assignment(m.topicId, m.partition);
        if (pa == nullptr) return false;
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
            if (added && n != m.leaderId && eligibleKnown_ &&
                std::find(eligible_.begin(), eligible_.end(), n) == eligible_.end()) {
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
        const int64_t key = partitionKey_(m.topicId, m.partition);
        if (inflight_.count(key) != 0) return false;   // decide on the committed state only
        for (const auto& q : pendingDynamicRecords_)
            if (q.rec.topicId == m.topicId && q.rec.partition == m.partition) return false;
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
    /// establishment of leadership gets a NEW epoch — commit the same
    /// assignment (leader, ISR, replicas) at leaderEpoch + 1.  The caller
    /// validated (leader, leaderEpoch, partitionEpoch) against the committed
    /// assignment, so a duplicate request (re-send) can never bump twice.
    template <typename Reply>
    bool bumpLeaderEpoch_(const rune::replication::AlterIsrMsg& m,
                          const rune::metadata::PartitionAssignment& pa, Reply& reply) {
        const int64_t key = partitionKey_(m.topicId, m.partition);
        if (inflight_.count(key) != 0) return false;   // decide on the committed state only
        for (const auto& q : pendingDynamicRecords_)
            if (q.rec.topicId == m.topicId && q.rec.partition == m.partition) return false;
        rune::metadata::BalancerRecord br;
        br.isr      = pa.isr;
        br.replicas = pa.replicas;
        br.rec.topicId        = m.topicId;
        br.rec.partition      = m.partition;
        br.rec.leaderNode     = pa.leaderNode;
        br.rec.leaderEpoch    = pa.leaderEpoch + 1;
        br.rec.partitionEpoch = pa.partitionEpoch + 1;   // invariant leaderEpoch <= partitionEpoch
        br.rec.isr      = rune::protocol::metadata::Int32ArrayView{br.isr.data(), static_cast<uint16_t>(br.isr.size())};
        br.rec.replicas = rune::protocol::metadata::Int32ArrayView{br.replicas.data(), static_cast<uint16_t>(br.replicas.size())};
        pendingDynamicRecords_.push_back(std::move(br));
        ++leaderEpochBumps_;
        reply(rune::replication::kAlterIsrOk, pa.partitionEpoch);
        drainDynamicQueue();
        return true;
    }
    [[nodiscard]] uint64_t leaderEpochBumpsForTest() const noexcept { return leaderEpochBumps_; }
    /// The controller decides only once its map reflects its whole log as of
    /// the claim (see controllerReady_).
    [[nodiscard]] bool controllerCaughtUpForTest() const noexcept { return controllerCaughtUp_; }

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
        if (!leaderReady_()) return false;

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

        // MEDIUM-2: the eligible set onAlterIsr filters additions against.
        eligible_.assign(eligibleNodes.begin(), eligibleNodes.end());
        eligibleKnown_ = true;

        // MEDIUM-B: coordinator partitions (controller-ISR path).
        const std::size_t coord = reconcileCoordinatorPartitions(eligibleNodes);
        if (configuredTopics_.empty()) return coord;

        rune::metadata::DynamicBalancerInputs in;
        in.cur       = &msm_.committedMap();
        in.liveNodes = eligibleNodes;   // eligibleNodes(), not raw liveNodes()
        in.topics    = configuredTopics_;
        in.positions = positions;
        in.maxPreferredElectionsPerCycle = maxPreferredElectionsPerCycle;
        in.maxRecordsPerCycle            = maxRecordsPerCycle;
        in.minIsr                        = minIsr_;   // commit-quorum failover floor
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

        rune::metadata::DynamicBalancer bal;
        std::vector<rune::metadata::BalancerRecord> recs = bal.rebalance(in);
        // A partition with a record in flight (e.g. a committed-ISR change from
        // AlterIsr) is decided again on the next tick, on the committed state —
        // never on a stale ISR.
        if (!inflight_.empty()) {
            std::erase_if(recs, [this](const rune::metadata::BalancerRecord& br) {
                return inflight_.count(partitionKey_(br.rec.topicId, br.rec.partition)) != 0;
            });
        }
        // Kafka broker fencing: a node the cluster considers dead leaves the
        // COMMITTED ISR of every partition whose leader is alive (the failover
        // records above handle dead leaders) — with HW = min over the ISR it
        // would otherwise hold the HW for replicaLagMaxMs.  It re-enters through
        // its leader's AlterIsr once it fetched back to the HW.
        in.cur->forEach([&](int32_t topicId, int32_t partition,
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
                       std::find(eligibleNodes.begin(), eligibleNodes.end(), n) != eligibleNodes.end();
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

        // The log's existing content changed (a KIP-279 truncation while
        // following): everything applied from the old content is stale — rebuild
        // the MSM from the committed log as it is now.
        if (replica_->logGeneration() != lastLogGen_) {
            lastLogGen_ = replica_->logGeneration();
            rebuildFromLog();
        }

        // Detect the leader edge (same pattern as TCS::loadable_): rebuild the
        // controller state from the log, then emit.
        const bool amLeader = leaderReady_();
        if (amLeader && !wasLeader_) {
            // Became leader: rebuild the MSM from the committed log so we have
            // the full assignment map before serving requests.
            rebuildFromLog();
            wasLeader_ = true;
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
            wasLeader_ = false;
            assigned_  = false;  // re-arm emit if we regain leadership later
            // Records decided under this leadership but not appended: the next
            // leader re-decides from the committed map.
            pendingDynamicRecords_.clear();
            inflight_.clear();
            // Follower: reset lastApplied_ so we re-apply from scratch if we
            // become leader again later.
            lastApplied_ = -1;
        }

        const rune::replication::Offset hw = replica_->highWatermark();
        for (auto it = inflight_.begin(); it != inflight_.end();)
            it = (it->second <= hw) ? inflight_.erase(it) : std::next(it);
        if (hw <= 0 || hw <= lastApplied_) {
            // Nothing new to apply.  Still call commit() so pending records
            // already decoded in a prior partial cycle get committed.
            msm_.commit(hw - 1);
            if (amLeader) maybeCatchUp_();
            // Still drain the dynamic queue even if the log is caught up.
            if (amLeader) drainDynamicQueue();
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
        if (amLeader) maybeCatchUp_();

        // Drain the dynamic assignment queue (bounded per cycle).
        if (amLeader) drainDynamicQueue();

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

        const rune::replication::Offset hw = replica_->highWatermark();

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
    /// This replica may append to the metadata log (the replica's produce gate).
    [[nodiscard]] bool leaderReady_() const noexcept {
        return replica_ != nullptr && replica_->acceptsProduce();
    }
    /// This replica may DECIDE (emit assignments, answer AlterIsr, rebalance):
    /// it leads AND its MSM reflects its whole log as of the claim.
    [[nodiscard]] bool controllerReady_() const noexcept {
        return leaderReady_() && controllerCaughtUp_;
    }
    /// The catch-up edge: the HW (and the MSM apply cursor) reached the
    /// claim-time LEO — the map is complete; the deferred claim action runs.
    void maybeCatchUp_() {
        if (controllerCaughtUp_ || !leaderReady_()) return;
        const rune::replication::Offset applied = lastApplied_ < 0 ? 0 : lastApplied_;   // -1 = nothing yet
        if (replica_->highWatermark() < claimLeo_ || applied < claimLeo_) return;
        controllerCaughtUp_ = true;
        emitStaticAssignmentIfNeeded();
    }

    void drainDynamicQueue() {
        if (pendingDynamicRecords_.empty()) return;
        if (!leaderReady_()) return;   // kept queued until this replica may append

        static constexpr std::size_t kEncBufSize = 16 * 1024;
        uint8_t encBuf[kEncBufSize];

        std::size_t cycleRecords = 0;
        std::size_t cycleBytes   = 0;

        while (!pendingDynamicRecords_.empty()) {
            if (cycleRecords >= kMaxRecordsPerBatch) break;

            const auto& br = pendingDynamicRecords_.front();
            const std::size_t n = br.rec.encode(encBuf, kEncBufSize);
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

    // Follower→Leader transition gate (mirrors TCS / GCS patterns).
    bool wasLeader_ = false;
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
    uint64_t leaderEpochBumps_   = 0;

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
