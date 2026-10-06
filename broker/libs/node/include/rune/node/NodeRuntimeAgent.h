#pragma once
// NodeRuntimeAgent.h — Composite duty-cycle agent (BR-7a, GR-2, GR-3).
//
// Composes the inner consensus agent (Agent& — either ConsensusAgent for
// single-node or NodeConsensusAgent for multi-node) with the ClientGateway,
// AeronPartitionTransport, per-partition PartitionReplicas, and the
// ReplicationMetadataBridge so that all run on the SAME AgentRunner thread
// (single-writer invariant).
//
// doWork(nowNs):
//   1. consensusAgent.doWork(nowNs)    — drives Election / IngressRing
//   2. bridge.doWork(leaderId, epoch)  — detects leader/epoch change; flips replica roles
//   3. transport.poll(nowNs)           — drains Aeron ingress; resolves pending pubs
//   4. for each replica: replica.doWork(nowNs) — ISR checks / follower fetch emission
//   5. gateway ? gateway->doWork(nowNs) : 0
//   6. return total workCount
//
// The bridge (step 2) reads leaderId + leadershipTermId from consensus (written
// by step 1) and applies role transitions BEFORE transport/replica steps (3-4)
// so the replicas are always in the correct role before they do work.
//
// Lifecycle: owned by BrokerNode; created in start(), destroyed in stop()
// (BEFORE the AgentRunner is destructed / the gateway is reset).
//
// Single-writer: all accesses to gateway, transport, replicas, and consensus
// state happen on the single AgentRunner thread. No locking needed; no per-cycle heap.

#include <rune/consensus/ConsensusAgent.h>  // defines rune::consensus::Agent
#include <rune/node/AdminControlPlane.h>
#include <rune/node/NodeConsensusAgent.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/node/GroupCoordinationService.h>
#include <rune/node/TransactionCoordinationService.h>
#include <rune/node/MetadataService.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/CleanerViewChannel.h>
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/OneToOneByteRing.h>
#include <rune/node/OwnerThreadTestTask.h>
#include <rune/node/LeaderObserver.h>
#include <rune/node/ControlledShutdown.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune_fr/FlightRecorder.h>
#include <rune/replication/Types.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/Log.h>
#include <spdlog/spdlog.h>
#include <rune/storage/OffsetMap.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/SlowTick.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/Gpid.h>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <type_traits>
#include <vector>

namespace rune::node {

class NodeRuntimeAgent final : public rune::consensus::Agent {
public:
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    // @param consensusAgent  Inner agent (ConsensusAgent or NodeConsensusAgent).
    //                        Must outlive this object.
    // @param election        Consensus Election — read leaderMemberId() + leadershipTermId()
    //                        to drive the metadata bridge.  Must outlive this object.
    // @param bridge          ReplicationMetadataBridge (may be nullptr for single-node
    //                        with no replication, null-safe). Non-owning.
    // @param transport       AeronPartitionTransport (may be nullptr). Non-owning.
    // @param registry        ReplicaRegistry (may be nullptr). Non-owning.
    // @param gateway         ClientGateway pointer (may be null; non-owning).
    //                        Must outlive this object.
    // @param groupService    GroupCoordinationService pointer (may be null). Non-owning.
    //                        Must outlive this object.
    // Full constructor (GR-2/GR-3/CG-3 wiring).
    // @param consensusAgent  Inner agent (ConsensusAgent or NodeConsensusAgent).
    //                        Must outlive this object.
    // @param election        Consensus Election — read leaderMemberId() + leadershipTermId()
    //                        to drive the metadata bridge. May be nullptr when bridge is nullptr.
    // @param bridge          ReplicationMetadataBridge (may be nullptr). Non-owning.
    // @param transport       AeronPartitionTransport (may be nullptr). Non-owning.
    // @param registry        ReplicaRegistry (may be nullptr). Non-owning.
    // @param gateway         ClientGateway pointer (may be null). Non-owning.
    // @param groupService    GroupCoordinationService pointer (may be null). Non-owning.
    // @param txnService        TransactionCoordinationService pointer (may be null). Non-owning.
    // @param metadataService   MetadataService pointer (may be null). Non-owning.
    //                          MD1 Task 4: drives MSM apply loop + routing-image publish.
    NodeRuntimeAgent(rune::consensus::Agent&            consensusAgent,
                     rune::consensus::Election*          election,
                     ReplicationMetadataBridge*          bridge,
                     rune::replication::AeronPartitionTransport* transport,
                     ReplicaRegistry*                    registry,
                     rune::gateway::ClientGateway*       gateway,
                     GroupCoordinationService*           groupService    = nullptr,
                     TransactionCoordinationService*     txnService      = nullptr,
                     MetadataService*                    metadataService = nullptr,
                     WorkerPool*                         workerPool      = nullptr) noexcept
        : consensus_(ConsensusRef::of(consensusAgent))
        , election_(election)
        , bridge_(bridge)
        , transport_(transport)
        , registry_(registry)
        , gateway_(gateway)
        , groupService_(groupService)
        , txnService_(txnService)
        , metadataService_(metadataService)
        , workerPool_(workerPool)
    {}

    // Backward-compatible constructor for single-node / tests without replication.
    NodeRuntimeAgent(rune::consensus::Agent&        consensusAgent,
                     rune::gateway::ClientGateway*  gateway,
                     GroupCoordinationService*      groupService    = nullptr,
                     TransactionCoordinationService* txnService     = nullptr,
                     MetadataService*                metadataService = nullptr) noexcept
        : consensus_(ConsensusRef::of(consensusAgent))
        , election_(nullptr)
        , bridge_(nullptr)
        , transport_(nullptr)
        , registry_(nullptr)
        , gateway_(gateway)
        , groupService_(groupService)
        , txnService_(txnService)
        , metadataService_(metadataService)
        , workerPool_(nullptr)
    {}

    NodeRuntimeAgent(const NodeRuntimeAgent&)            = delete;
    NodeRuntimeAgent& operator=(const NodeRuntimeAgent&) = delete;

    // -----------------------------------------------------------------------
    // Task 8a: admin control-plane servicing hook (RULING-2 B).
    //
    // The schema DDL control plane hands off to the CONTROL thread via rings:
    //   - createReqRing (admin → control): create physical topic + add a user
    //     PartitionReplica (topic visibility via the ReplicaRegistry generation
    //     → WorkerPool::reconcile mechanism); reply the authoritative id.
    //   - egressRing (admin → control): send DDL response frames on the session
    //     egress via gateway->sendControlFrame.
    //   - roleRing (control → admin): posted here on leadership change.
    //
    // BrokerNode wires its concrete AdminControlPlane; it runs on THIS (control)
    // thread every cycle as a DIRECT call (Task 11 fix 1: no std::function /
    // virtual on the duty cycle — under fused placement it is worker 0's pass).
    // Non-owning; single-writer (control thread).  Null when no admin plane.
    // -----------------------------------------------------------------------
    void setAdminControl(AdminControlPlane* control) noexcept { adminControl_ = control; }

    /// This node's id: enables the per-cycle leadership hand-off to the gateway
    /// (BrokerNode sets it; unset = the gateway keeps its LeaderView callable).
    void setSelfId(int32_t selfId) noexcept { selfId_ = selfId; }

    // Task 11 fix 1: which concrete consensus agent this runtime drives.  Single
    // / Multi = a direct (final-class) call per cycle; Generic = virtual, only for
    // test doubles.  BrokerNode always builds Single or Multi.
    enum class ConsensusKind : uint8_t { Single, Multi, Generic };
    [[nodiscard]] ConsensusKind consensusKind() const noexcept { return consensus_.kind; }

    /// F9a: the control-thread → observer leadership ring.  Set before start.
    void setLeaderObserver(LeaderObserver* o) noexcept { leaderObserver_ = o; }

    int drainNodeControl_() {
        int sent = 0;
        if (auto& out = metadataService_->nodeControlOutbox(); !out.empty()) [[unlikely]] {
            std::size_t n = 0;
            while (n < out.size() && transport_->sendAlterIsr(out[n])) ++n;
            out.erase(out.begin(), out.begin() + static_cast<std::ptrdiff_t>(n));
            sent += static_cast<int>(n);
        }
        if (auto& out = metadataService_->nodeControlResponseOutbox(); !out.empty()) [[unlikely]] {
            std::size_t n = 0;
            while (n < out.size() && transport_->sendAlterIsrResponse(out[n].first, out[n].second)) ++n;
            out.erase(out.begin(), out.begin() + static_cast<std::ptrdiff_t>(n));
            sent += static_cast<int>(n);
        }
        return sent;
    }

    /// Controlled shutdown state machine (driven on this agent's slow tick).
    /// BrokerNode binds it before the control thread starts; begin()/observe()
    /// are its cross-thread ring endpoints.
    ControlledShutdown& controlledShutdown() noexcept { return controlledShutdown_; }

    // fsyncgate fail-stop (Task 10 fix 2 / I-1): replicas this control thread
    // drives (internal partitions; all partitions without a WorkerPool) whose
    // log fsync failed are reported here ONCE → BrokerNode halts (exit 74).
    using StorageFailureHook = std::function<void(const std::string& logDir, int err)>;
    void setStorageFailureHook(StorageFailureHook hook) { storageFailureHook_ = std::move(hook); }

    // -----------------------------------------------------------------------
    // Agent::doWork — single-writer duty cycle.
    // Returns sum of work done by all components.
    // -----------------------------------------------------------------------
    const char* name() const noexcept override { return "rune-runtime"; }

    int doWork(int64_t nowNs) override {
        // F7 ownership invariant (WorkerPool): a runtime replica move is a hard
        // error → halt (no exception escapes onto the AgentRunner thread).
        try {
            return doWorkImpl_(nowNs);
        } catch (const std::logic_error& e) {
            if (!fatalReported_) {
                fatalReported_ = true;
                spdlog::critical("NodeRuntimeAgent: invariant violation: {} — halting", e.what());
                if (fatalHook_) fatalHook_(e.what());
            }
            return 0;
        }
    }

    // Task 10 round 4: the LogCleanerAgent's replica view channel (rings only).
    // BrokerNode calls this before any runner starts and attaches the result to
    // the cleaner.  The channel lives here (outlives the cleaner on shutdown).
    CleanerViewChannel* enableCleanerView() {
        if (!cleanerChannel_) cleanerChannel_ = std::make_unique<CleanerViewChannel>();
        return cleanerChannel_.get();
    }
    [[nodiscard]] std::uint64_t cleanerAckedGenForTest() const noexcept { return cleanerAckedGen_; }

    /// Aeron's io.aeron.cluster.ConsensusModuleAgent.SLOW_TICK_INTERVAL_NS.
    static constexpr int64_t kSlowTickIntervalNs = 10'000'000;   // 10 ms

    /// Task 11 (fused placement): run the O(partitions) failover / rebalance scan
    /// as a slow tick every `ns` instead of every duty cycle.  0 (default, thread
    /// placement) = every cycle.  Set before the agent runs.
    void setSlowTickIntervalNs(int64_t ns) noexcept { failoverTick_.intervalNs = ns > 0 ? ns : 0; }

    /// Test-only (this agent's thread): duty cycles run / routing images published.
    [[nodiscard]] std::uint64_t cyclesForTest() const noexcept { return cycles_; }
    [[nodiscard]] std::uint64_t routingImagePublishesForTest() const noexcept { return routingImagePublishes_; }

    // Halt on an invariant violation (BrokerNode wires it; called once).
    void setFatalHook(std::function<void(const std::string&)> hook) { fatalHook_ = std::move(hook); }

private:
    int doWorkImpl_(int64_t nowNs) {
        RUNE_FR_SPAN_RUNTIME(rune_fr::Site::RuntimeDoWork, 0);
        ++cycles_;
        // 1. Consensus (Election + IngressRing drain).
        int work = consensus_.doWork(nowNs);   // direct call (Single / Multi), never virtual in BrokerNode
        // Task 11 fix 1: hand the gateway this cycle's leadership as a plain value
        // (was a per-cycle std::function LeaderView — which also built a
        // std::string on followers).  Same value BrokerNode::isLeader() reads.
        if (selfId_ >= 0) {
            const bool leader = isLocalLeader_();
            if (gateway_)      gateway_->setLocalLeadership(leader);
            if (groupService_) groupService_->setLocalLeadership(leader);   // fix 2 (B1)
            if (txnService_)   txnService_->setLocalLeadership(leader);     // fix 2 (B1)
        }
        // 1a. F9a: publish the leadership view to the observer thread (ring,
        //     only on change) right after the consensus step made it current.
        if (leaderObserver_) leaderObserver_->publishOnControl();

        // 1b. Task 10 fix 3 (I2a): fan ownership out BEFORE anything this cycle
        //     can route ops (bridge role ops, transport poll, gateway), and free
        //     removed replicas no worker can still reference (all acked).
        if (workerPool_ && registry_) workerPool_->reconcile(*registry_);
        if (registry_) {
            // Round 4: the off-thread cleaner holds replica pointers too (its own
            // ring-delivered view) — a retired replica is freed only once every
            // worker AND the cleaner acked a generation at or above its removal.
            work += publishCleanerView_();
            std::uint64_t safe = workerPool_ ? workerPool_->minAckedOwnershipGen()
                                             : registry_->generation();
            if (cleanerChannel_ && cleanerAckedGen_ < safe) safe = cleanerAckedGen_;
            registry_->collectRetired(safe);
        }

        // 2. Metadata bridge — detect leader/epoch change and flip replica roles.
        //    Reads election_ AFTER consensus step so the state is current.
        if (bridge_ && election_) {
            const rune::consensus::NodeId  lid   = election_->leaderMemberId();
            const rune::replication::Epoch epoch =
                static_cast<rune::replication::Epoch>(election_->leadershipTermId());
            work += bridge_->doWork(lid, epoch);
            // AlterIsr proposals route to the CURRENT controller (consensus leader).
            if (transport_) transport_->setControllerId(static_cast<int32_t>(lid));
            // M1: an invariant violation caught at the bridge's noexcept
            // boundary joins the uniform halt path below (exit 70).
            if (std::string what; bridge_->takeFatal(what)) throw std::logic_error(what);
        }

        // 2b. Metadata service — apply committed assignment records.
        //     MD1 Task 4: drives MetadataStateMachine apply loop.
        //     Runs right after the bridge so the replica's role is already
        //     correct before we check highWatermark() (leader vs follower gate).
        //     FINDING-1: the metadata replica's role is driven by cluster
        //     leadership (bridge), NOT the assignment map — this is correct.
        //
        //     MD1 Task 9: After MetadataService::doWork() returns, rebuild the
        //     RoutingImage from the committed AssignmentMap and push it to the
        //     gateway's routingImage_ snapshot.  This is the capture-once path:
        //     we build the image once per duty cycle (on the single-writer thread)
        //     and the gateway uses it for ALL per-produce/subscribe lookups within
        //     that cycle.  Zero atomic_load per produce on the hot path.
        if (metadataService_) {
            const int metaWork = metadataService_->doWork(nowNs);
            work += metaWork;
            // Controlled shutdown (cold; two empty() checks when idle): member
            // confirmations → the controller, the controller's pushed COMPLETE
            // → the stopping node.  Every cycle (events, not the slow tick).
            if (election_) metadataService_->noteController(static_cast<int32_t>(election_->leaderMemberId()));
            if (transport_) [[likely]] work += drainNodeControl_();

            // GR-7 / Task-14: FAILOVER reassignment of partition leadership.
            //
            // After the committed assignment map has been applied, ask the
            // MetadataService (leader-only, self-gated) to reassign any partition
            // whose designated leaderNode is no longer among the LIVE cluster
            // members.  Liveness comes from consensus itself (Election's per-member
            // AppendPosition timestamps) — the authoritative signal for "which
            // node is dead", read on this same duty-cycle thread (no race).
            //
            // Without this, a killed node's partitions stay assigned to it in the
            // committed map forever, so the surviving nodes' replicas never get
            // promoted to Leader (GR-7 / TX-2b failover would hang).
            //
            // Task 11: O(partitions) — a SLOW-TICK step (Aeron ConsensusModule-
            // Agent::slowTickWork): due every failoverTick_.intervalNs, and again
            // on the very next cycle while it still emits records.  Interval 0
            // (thread placement) = every cycle, exactly as before.
            if (election_ && failoverTick_.due(nowNs)) {
                int slowWork = 0;
                // BIDIRECTIONAL partition-leadership rebalance on eligible-set change.
                //
                // Failure detection uses the consensus heartbeat eligible set (the
                // only real-time "is it dead" signal the metadata leader has; FLP
                // makes some timeout irreducible for crash detection).  The
                // TIME-INDEPENDENT part is CONVERGENCE: we run the rebalance on ANY
                // change of the eligible set — a SHRINK fails a dead leader's
                // partitions over to a survivor, a GROW restores balance (preferred-
                // leader elections move leadership back toward the round-robin
                // assignment when the returned node is in ISR).  So a transient
                // heartbeat false-positive can no longer permanently collapse
                // leadership: the moment the node reappears, leadership re-converges,
                // for ANY window (no tuned grace).  reassign is idempotent, so a
                // steady eligible set emits nothing.
                //
                // ALLOCATION-FREE per cycle: the liveness poll fills a fixed stack
                // buffer (no std::vector return); the eligible vector handed to the
                // balancer is a REUSED member (capacity persists → no steady-state
                // heap alloc).
                const int64_t failoverTimeoutNs =
                    3 * election_->leaderHeartbeatTimeoutNs();
                const std::size_t n = election_->eligibleNodeIds(
                    nowNs, failoverTimeoutNs, eligBuf_.data(), eligBuf_.size());

                // Kafka broker fencing for INTERNAL partitions (led here, leader-
                // local ISR): a member the cluster considers dead leaves the ISR
                // at once — with HW = min over the ISR it would otherwise hold the
                // HW (metadata commits!) for replicaLagMaxMs.
                if (n > 0 && registry_) {
                    for (auto* replica : registry_->allReplicas()) {
                        if (replica && replica->topicId() < 0 &&
                            replica->role() == rune::replication::Role::Leader)
                            replica->shrinkIsrToLive(
                                std::span<const int32_t>(eligBuf_.data(), n), nowNs);
                    }
                }

                // Track eligible-set changes (kept for diagnostics / future gating).
                const bool changed =
                    !prevEligInit_ || n != prevEligLen_ ||
                    !std::equal(eligBuf_.begin(), eligBuf_.begin() + n,
                                prevEligBuf_.begin());
                // TEMP control-plane churn diagnostic (RUNE_CTRL_LOG=1).
                if (ctrlLog_) {
                    const int32_t curLeader = election_->leaderMemberId();
                    const int64_t curTerm   = (int64_t)election_->leadershipTermId();
                    if (changed || curLeader != dbgLastLeader_ || curTerm != dbgLastTerm_) {
                        char eb[256]; int off = 0;
                        for (std::size_t i = 0; i < n && off < 240; ++i)
                            off += std::snprintf(eb + off, sizeof(eb) - off, "%d ",
                                                 (int)eligBuf_[i]);
                        eb[off] = '\0';
                        std::fprintf(stderr,
                            "[CTRL] t=%lldms consensusLeader=%d term=%lld eligN=%zu elig=[%s] "
                            "(prevLeader=%d prevTerm=%lld changed=%d)\n",
                            (long long)(nowNs / 1'000'000), (int)curLeader,
                            (long long)curTerm, n, eb, (int)dbgLastLeader_,
                            (long long)dbgLastTerm_, (int)changed);
                        dbgLastLeader_ = curLeader;
                        dbgLastTerm_   = curTerm;
                    }
                }
                if (n > 0) {
                    // Refresh the controller's ReplicaPositionTable from the worker-
                    // published, race-free controller position snapshots BEFORE the
                    // rebalance runs, so the DynamicBalancer decides leadership
                    // restoration off POSITIONS (a returned replica caught up to the
                    // leader's LEO) rather than a heartbeat-timeout race.  Acquire-load
                    // only — no access to live worker-owned state.
                    //
                    // CONVERGENCE (time-independent): we run the refresh + idempotent
                    // reassign on EVERY scan (every duty cycle under thread placement;
                    // every slow tick — and the very next cycle after any emit —
                    // under fused placement), not only on an eligible-set change.
                    // A returned replica's position becomes visible some cycles AFTER
                    // it re-enters the eligible set (the leader must observe its next
                    // fetch), so a one-shot on-change reassign would miss the
                    // restoration and leave the ISR permanently collapsed for those
                    // partitions.  reassign is idempotent (emits nothing once every
                    // partition has a live, in-ISR leader), so re-running each cycle
                    // costs an O(partitions) no-op at steady state and converges for
                    // ANY position-arrival ordering, with no tuned grace window.
                    if (registry_ && metadataService_) {
                        for (auto* replica : registry_->allReplicas()) {
                            if (!replica) continue;
                            const int32_t ep = replica->controllerLeaderEpoch();
                            if (ep < 0) continue;  // never published yet
                            const int32_t tid  = replica->topicId();
                            const int32_t part = replica->partitionId();
                            for (int32_t node = 0;
                                 node < rune::replication::PartitionReplica::kMaxControllerNodes;
                                 ++node) {
                                const auto leo = replica->controllerReplicaLeo(node);
                                if (leo < 0) continue;  // unknown for this node
                                metadataService_->reportReplicaPosition(
                                    tid, part, node, ep, leo);
                            }
                        }
                    }
                    eligibleScratch_.assign(eligBuf_.begin(), eligBuf_.begin() + n);
                    // Two-speed convergence:
                    //   • EVERY cycle → FAILOVER-ONLY (maxPreferred=0): fail a
                    //     genuinely-unavailable leader over to a survivor and re-seed/
                    //     expand the ISR.  Idempotent, no churn.
                    //   • On an eligible-set CHANGE (rare) → also allow the PREFERRED-
                    //     LEADER BALANCE pass (maxPreferred>0) so leadership SPREADS
                    //     back off the survivor once nodes return — otherwise all
                    //     partitions stay collapsed on the single consensus leader and
                    //     that one node's followers can't sustain the produce rate
                    //     (throughput bottleneck).  Gating balance to on-change (not
                    //     every cycle) keeps it from oscillating against a heartbeat-
                    //     flapping node while still re-spreading load when the set is
                    //     stable.  restorable()/position-gating keeps every move safe.
                    //   • Epoch-restart fix round 1 (M2): also when the controller
                    //     just caught up after its claim, or the committed map changed
                    //     since the last pass (a returned replica re-entered an ISR —
                    //     it becomes a preferred candidate only THEN, after the
                    //     eligible-set edge passed).  An edge seen while the
                    //     controller cannot decide is NOT consumed (below).
                    const bool ctlReady = metadataService_->controllerReady();
                    const bool recheck  = metadataService_->takePreferredRecheck();
                    const int maxPref = (changed || recheck) ? 1024 : 0;
                    const std::size_t emitted =
                        metadataService_->reassignDeadPartitionLeaders(
                            eligibleScratch_, maxPref);
                    if (ctrlLog_ && emitted > 0)
                        std::fprintf(stderr,
                            "[CTRL] t=%lldms REASSIGN emitted=%zu records "
                            "(maxPref=%d eligN=%zu) — a partition leader was deemed "
                            "dead/rebalanced\n",
                            (long long)(nowNs / 1'000'000), emitted, maxPref, n);
                    work += static_cast<int>(emitted);
                    slowWork += static_cast<int>(emitted);
                    if (ctlReady) {   // M2: never consume the edge undecided
                        std::copy(eligBuf_.begin(), eligBuf_.begin() + n,
                                  prevEligBuf_.begin());
                        prevEligLen_  = n;
                        prevEligInit_ = true;
                    }
                }
                // Controlled shutdown (cold): register / hand-off steps, on
                // the same slow tick as the rebalance it depends on.
                slowWork += controlledShutdown_.doWork(nowNs);
                failoverTick_.done(nowNs, slowWork);
            }

            // Rebuild the RoutingImage and push it to the gateway WHEN ITS INPUTS
            // CHANGE (MetadataService::routingVersion(): committed map or the
            // topic+node config) — Task 11: never per cycle (a heap allocation +
            // an O(partitions) rebuild on every runtime cycle, which under fused
            // placement is every worker-0 pass).  Done on the duty-cycle thread —
            // the gateway reads the image on the SAME thread (no race).
            //
            // BOOTSTRAP FIX (MD1-13): a RoutingImage is published from the very
            // first duty cycle (the first cycle always builds).
            //
            // Two-path strategy:
            //
            // STEADY STATE (committedMap non-empty):
            //   Build from the committed assignment map and push to the gateway.
            //   This is Path A — per-partition routing using the authoritative
            //   committed assignment (driven by MetadataStateMachine).
            //
            // BOOTSTRAP WINDOW (committedMap empty — metadata log not yet replicated):
            //   Build from StaticBalancer directly (deterministic: same output on
            //   ALL nodes given the same topic+node config wired at startup).
            //   Publishing the static routing image immediately means ALL nodes
            //   (Raft leader AND followers) have routingImage_ set from the very
            //   first duty cycle — eliminating the asymmetry window where the
            //   Raft leader (which gets committedMap first) redirects to a
            //   follower whose routingImage_ is still null, causing a redirect loop
            //   (Raft leader Path A → follower Path B → Raft leader → exhausted).
            //
            //   If buildStaticRoutingImage() returns null (no topics configured,
            //   or BalancerError), leave routingImage_ null → Path B (pre-MD1
            //   cluster-leader fallback) serves the produce.
            const std::uint64_t routingVersion = metadataService_->routingVersion();
            if (gateway_ && routingVersion != routingVersionBuilt_) {
                routingVersionBuilt_ = routingVersion;
                const auto& cmap = metadataService_->committedMap();
                // Coordinator partitions (re-review MEDIUM-B) are in the map
                // too: "committed metadata ready" means a USER assignment.
                // Cold (routing-version change only).
                bool anyUser = false;
                cmap.forEach([&](int32_t tid, int32_t, const rune::metadata::PartitionAssignment&) {
                    anyUser = anyUser || tid >= 0;
                });
                if (anyUser) {
                    // Steady state: use the committed assignment map.
                    auto img = rune::metadata::RoutingImage::buildFromMap(
                        cmap, rune::metadata::gpidOf);
                    gateway_->setRoutingImageSnapshot(std::move(img));
                    ++routingImagePublishes_;
                    // Signal that committed metadata is available.
                    // GR-4 (per-partition NotLeader defence) and GR-5 (ISR gate)
                    // only activate in steady state so that the bootstrap window
                    // (replica still in Follower role before bridge Step B fires)
                    // does not falsely reject produces.
                    gateway_->setCommittedMetadataReady(true);
                } else {
                    // Bootstrap window: build from StaticBalancer (deterministic).
                    // All nodes compute the same image; no replication wait needed.
                    auto img = metadataService_->buildStaticRoutingImage();
                    if (img) {
                        gateway_->setRoutingImageSnapshot(std::move(img));
                        ++routingImagePublishes_;
                    }
                    // committedMetadataReady_ remains false → GR-4 / GR-5 suppressed.
                    // If null (no topics / BalancerError): leave routingImage_ null too.
                }
            }
        }

        // 3. Group coordination service — drive after bridge (sees current leadership).
        //    Called BEFORE transport/gateway so membership state is current before
        //    any ingress group messages are processed by gateway.doWork.
        if (groupService_) {
            work += groupService_->doWork(nowNs);
        }

        // 3b. Transaction coordination service — drive alongside group service.
        //     TX-3: drives timeout aborts; pending marker resolution (Task 8).
        if (txnService_) {
            work += txnService_->doWork(nowNs);
        }

        // 4. Transport poll — drain inbound Aeron frames, resolve pending pubs.
        if (transport_) {
            work += transport_->poll(nowNs);
        }

        // 5 + 6.5. Per-replica duty cycle + disk-log cleaner/retention drains.
        //
        // E2 inc2b: workers self-drive on their own AgentRunner threads.
        // The control thread MUST NOT call workerPool_->doWork() — that would
        // double-drive and cause data races.
        //
        // SHARED-NOTHING SPLIT:
        //   USER partitions (topicId >= 0): owned + driven by workers.
        //   INTERNAL partitions (topicId < 0): driven by the control thread here.
        //     Rationale: MetadataService, GroupCoordinationService, and
        //     TransactionCoordinationService all run on the control thread and
        //     read/write the internal replicas (role(), doWork, becomeLeader/
        //     becomeFollower).  Keeping internal replicas on the control thread
        //     eliminates the MetadataService::doWork ↔ Worker::becomeLeader race
        //     (second TSan-confirmed race, same binary).
        //
        // What the control thread DOES do (always, when workerPool_ is wired):
        //   reconcile() — re-derives shard→worker ownership when the registry
        //   changes (O(1) generation check; re-derive only on change).
        //   Ownership publish is control-thread-only; workers read it lock-free.
        //   doWork + log-drains for INTERNAL partitions.
        //
        // When workerPool_ is null (single-thread mode, passthrough, tests
        // without a pool), fall back to the pre-E1 registry loop unchanged.
        if (workerPool_) {
            if (registry_) {
                workerPool_->reconcile(*registry_);
                // Drive internal (topicId < 0) replicas on the control thread.
                // Workers self-drive USER partitions — do NOT call
                // workerPool_->doWork() here.
                for (auto* replica : registry_->allReplicas()) {
                    if (replica && replica->topicId() < 0) {
                        work += replica->doWork(nowNs);
                        checkReplicaStorage_(replica);
                    }
                }
            }
        } else if (registry_) {
            // Pre-E1 path: iterate registry directly (all replicas).
            for (auto* replica : registry_->allReplicas()) {
                if (replica) {
                    work += replica->doWork(nowNs);
                    checkReplicaStorage_(replica);
                }
            }
        }

        // 5.9. Task 8a: service the schema DDL control plane (RULING-2 B).
        //      Drains admin→control rings (createTopic requests + response egress)
        //      and posts leadership role events to the admin thread.  Runs BEFORE
        //      gateway_->doWork so any DDL response frame enqueued via
        //      sendControlFrame is flushed on the egress this same cycle.
        if (adminControl_) {
            work += adminControl_->doWork(nowNs);
        }

        // 6. Gateway.
        if (gateway_) {
            work += gateway_->doWork(nowNs);
        }

        // 6.5. CP-6: Drain pending cleaner swaps from LogCleanerAgent.
        //      LogCleanerAgent runs on its own thread and enqueues compaction
        //      outputs via Log::enqueueCleanedGroup().  This single-writer thread
        //      validates and installs them in memory (no filesystem op, one group
        //      per partition per pass) — PartitionReplica::drainCleanerSwapsOnOwner.
        //
        //      CP-9: Also drain retention PROPOSALS (Log::enqueueDeletedSegment):
        //      this thread validates each one (incarnation, not active, ends <=
        //      HW) and only then renames its files to *.deleted and erases it
        //      (PartitionReplica::drainRetentionDeletesOnOwner).
        //
        //      E1/E2: when workerPool_ is wired:
        //        - INTERNAL replicas: drained here (control thread owns them).
        //        - USER replicas: Worker::doWork already handles these drains
        //          for each owned shard. Skip them here to avoid double-draining.
        if (workerPool_ && registry_) {
            for (auto* replica : registry_->allReplicas()) {
                // Only internal replicas — user replicas are drained by workers.
                if (replica && replica->topicId() < 0 && replica->isDiskBacked()) {
                    work += replica->drainCleanerSwapsOnOwner();
                    work += replica->drainRetentionDeletesOnOwner();
                    work += replica->log()->drainStagingRenamesDone();
                }
            }
        } else if (!workerPool_ && registry_) {
            for (auto* replica : registry_->allReplicas()) {
                // Task M1 precondition #2: skip memory-backed replicas.
                // drainCleanerSwaps/drainRetentionDeletes/drainStagingRenamesDone are
                // disk-Log-only operations; replica->log() asserts isDiskBacked().
                if (replica && replica->isDiskBacked()) {
                    work += replica->drainCleanerSwapsOnOwner();
                    work += replica->drainRetentionDeletesOnOwner();
                    // Append-p99 fix: apply completed staging→base renames the
                    // background (cleaner) thread performed, so logPath_ tracks the
                    // on-disk name (append-thread-only segments_ access).
                    work += replica->log()->drainStagingRenamesDone();
                }
            }
        }

        // F1: TEST-ONLY owner-thread hook.  One empty-ring poll per CONTROL cycle
        // (never on the worker hot path); the dispatch body is out-of-line + cold.
        work += drainOwnerThreadTestTasks_();

        return work;
    }

public:

    // -----------------------------------------------------------------------
    // F1: TEST-ONLY owner-thread hook — producer side (TEST thread).
    //
    // Offer a ring-carried OwnerThreadTestTask ref (see OwnerThreadTestTask.h)
    // to the CONTROL thread over testTaskRing_ (F9a: a many-to-one ControlRing,
    // so any number of test threads may offer with NO lock; single consumer =
    // this control thread).  Returns false if the ring is full (caller fails
    // loudly).
    // -----------------------------------------------------------------------
    /// Shutdown (the controller, AFTER the runtime's AgentRunner was joined):
    /// skip every test task still queued so no test thread waits forever.
    void skipQueuedOwnerThreadTestTasks() noexcept {
        for (void* ref : pendingTestTasks_) skipOwnerThreadTestTask(ref);
        pendingTestTasks_.clear();
        (void)testTaskRing_.poll([](int32_t, std::int64_t a, std::int64_t) noexcept {
            skipOwnerThreadTestTask(reinterpret_cast<void*>(static_cast<std::uintptr_t>(a)));
        });
    }

    [[nodiscard]] bool offerOwnerThreadTestTask(void* ref) noexcept {
        return testTaskRing_.offer(kTestTaskMsgTypeId,
                                   static_cast<std::int64_t>(reinterpret_cast<std::uintptr_t>(ref)));
    }

    // -----------------------------------------------------------------------
    // runBoundedCompaction — CP-2/CP-3: bounded, leader-only compaction pass
    //   with full cleanup.policy routing.
    //
    // CP-9 NOTE: Retention (size / time / log-start-offset) has been MOVED to
    //   LogCleanerAgent (runs on its own cadence off the append thread).
    //   runBoundedCompaction only handles the legacy inline compaction path
    //   (used by CompactionDutyCycleTest and CompactionPolicyTest which test
    //   the old duty-cycle-driven path).  The Delete/CompactDelete retention
    //   portion is still here for backward-compatibility with existing tests
    //   that call runBoundedCompaction directly; those tests use deleteOldSegments
    //   which is a direct synchronous call (not the async-delete path).
    //   Production code should use LogCleanerAgent for retention.
    //
    // CP-3 policy routing (all paths are leader-only):
    //
    //   Delete       → run size/time retention (deleteOldSegments) ONLY.
    //                  Never compacts (delete-policy logs are not keyed changelogs).
    //
    //   Compact      → run compaction (latest-per-key) ONLY.
    //                  Never calls deleteOldSegments: compacted keyed changelogs
    //                  must never be size/time-aged because live keys must survive
    //                  past any size cap.
    //
    //   CompactDelete → run compaction FIRST (compact pass de-duplicates keys),
    //                  THEN run size/time retention.  Ordering matters: compact
    //                  before delete ensures the deduplicated-key survivor is not
    //                  incorrectly removed by the retention pass.
    //
    // Returns number of work units performed (>0 if any work done, 0 when idle).
    //
    // Single-writer invariant: called on the AgentRunner duty-cycle thread only.
    // NEVER touches the active segment (deleteOldSegments always keeps segments_.size()>1).
    // -----------------------------------------------------------------------
    int runBoundedCompaction(int64_t nowNs) {
        if (!registry_) return 0;

        int work = 0;
        for (auto* replica : registry_->allReplicas()) {
            if (!replica) continue;

            // 1. Leader-only.
            if (replica->role() != rune::replication::Role::Leader) continue;

            const auto policy = replica->cleanupPolicy();

            // 3. Need a disk-backed log — skip memory-backed partitions (e.g.
            //    test-only InMemoryLog replicas).  Calling replica->log() on a
            //    memory-backed partition asserts isDiskBacked(); guard here so
            //    the invariant holds universally (precondition #2 completeness).
            if (!replica->isDiskBacked()) continue;
            rune::storage::Log* log = replica->log();
            if (!log) continue;

            // ---------------------------------------------------------------
            // CP-3: Delete policy — retention ONLY, no compaction.
            // ---------------------------------------------------------------
            if (policy == rune::replication::CleanupPolicy::Delete) {
                // Run size retention when retentionBytes is set (>= 0).
                const int64_t retBytes = replica->retentionBytes();
                if (retBytes >= 0) {
                    const int deleted = log->deleteOldSegments(retBytes, replica->highWatermark());
                    work += deleted;
                }
                continue; // done for this replica — never compact Delete topics
            }

            // ---------------------------------------------------------------
            // Compact or CompactDelete: run the compaction pass first.
            // ---------------------------------------------------------------

            // 4. Must have at least 2 segments (one sealed, one active).
            const auto bases = log->segmentBaseOffsets();
            if (bases.size() < 2) {
                // No sealed segments — skip compaction, but still run retention
                // for CompactDelete if configured.
                if (policy == rune::replication::CleanupPolicy::CompactDelete) {
                    const int64_t retBytes = replica->retentionBytes();
                    if (retBytes >= 0) {
                        work += log->deleteOldSegments(retBytes, replica->highWatermark());
                    }
                }
                continue;
            }

            // The active segment is the LAST one (highest base offset).
            const int64_t activeBase = bases.back();

            // 5. Dirty-ratio gate.
            //
            // Dirty range = [lastCleanOffset, activeBase).
            // Dirty ratio = (activeBase - firstDirty) / max(1, activeBase).
            const int64_t firstDirty = replica->lastCleanOffset();
            const double dirtyRatio =
                static_cast<double>(activeBase - firstDirty) /
                static_cast<double>(activeBase > 0 ? activeBase : 1);

            bool compactionRan = false;
            if (dirtyRatio > replica->minCleanableDirtyRatio()) {
                // 6. Find the next sealed segment to compact: the first sealed segment
                //    whose base offset >= firstDirty.  One-segment-per-cycle bound.
                int64_t segToCompact = -1;
                int64_t segEnd       = -1;

                for (std::size_t i = 0; i + 1 < bases.size(); ++i) {
                    if (bases[i] < firstDirty) continue;
                    segToCompact = bases[i];
                    segEnd       = bases[i + 1];
                    break;
                }

                if (segToCompact < 0) {
                    // All sealed segments already clean — advance watermark.
                    replica->setLastCleanOffset(activeBase);
                } else {
                    // 7. Compact exactly this ONE sealed segment via bounded cleaner.
                    rune::storage::LogCleaner cleaner;
                    const bool cleaned = cleaner.cleanSegment(*log,
                                         /*segBase=*/segToCompact,
                                         /*segEnd=*/segEnd,
                                         /*globalDirtyStart=*/firstDirty,
                                         nowNs,
                                         replica->deleteRetentionMs());

                    // 8. Advance the cleaned watermark ONLY over a segment that
                    //    was actually compacted (E1d F4): a segment refused by
                    //    the recovery-point gate (it ends above the persisted R)
                    //    stays dirty and is retried once its checkpoint is
                    //    durable.  An EMPTY sealed segment (nothing to rewrite:
                    //    cleanSegment false, gate open) is passed over — never a
                    //    watermark wedged on it forever.
                    if (cleaned || (log->compactionAllowedThrough(segEnd) &&
                                    log->segmentNextOffsetAt(segToCompact) == segToCompact)) {
                        replica->setLastCleanOffset(segEnd);
                        ++work;
                        compactionRan = true;
                    }
                }
            }

            // ---------------------------------------------------------------
            // CP-3: CompactDelete — after compaction, run size retention.
            // Ordering: compact FIRST (already done above), THEN delete old segments.
            // ---------------------------------------------------------------
            if (policy == rune::replication::CleanupPolicy::CompactDelete) {
                const int64_t retBytes = replica->retentionBytes();
                if (retBytes >= 0) {
                    const int deleted = log->deleteOldSegments(retBytes, replica->highWatermark());
                    work += deleted;
                }
            }
            // Suppress unused-variable warning when compactionRan is not used below.
            (void)compactionRan;
        }
        return work;
    }

private:
    // -----------------------------------------------------------------------
    // F1: TEST-ONLY owner-thread hook — consumer side (THIS control thread).
    //
    // For each ring-carried task ref, resolve the replica in THIS thread's
    // ReplicaRegistry (the registry is control-thread-owned) and route to the
    // replica's TRUE owner, mirroring the ownership split in doWork above:
    //   - ControlThread target, internal partition (topicId < 0), or no
    //     WorkerPool (pre-E1: control drives every replica)  → run inline HERE.
    //   - user partition with a WorkerPool → WorkerOp::TestTask over the
    //     EXISTING control→worker ingress ring (this thread is its sole
    //     producer); the owning worker runs it in its duty cycle.  Ring full →
    //     hold in pendingTestTasks_ (FIFO) and retry next cycle; never drop.
    // Missing replica → run inline with nullptr (the task reports the error).
    // -----------------------------------------------------------------------
    int drainOwnerThreadTestTasks_() {
        int work = 0;
        if (!pendingTestTasks_.empty()) work += retryPendingOwnerThreadTestTasks_();
        work += testTaskRing_.poll(
            [this](int32_t /*typeId*/, std::int64_t a, std::int64_t /*b*/) {
                void* ref = reinterpret_cast<void*>(static_cast<std::uintptr_t>(a));
                // Keep FIFO behind held ones; hold (never drop) on a full ingress.
                if (!pendingTestTasks_.empty() || !dispatchOwnerThreadTestTask_(ref)) {
                    pendingTestTasks_.push_back(ref);
                }
            }, 16);
        return work;
    }

    [[gnu::cold]] [[gnu::noinline]] int retryPendingOwnerThreadTestTasks_() {
        std::size_t i = 0;
        for (; i < pendingTestTasks_.size(); ++i) {
            if (!dispatchOwnerThreadTestTask_(pendingTestTasks_[i])) break;
        }
        pendingTestTasks_.erase(pendingTestTasks_.begin(),
                                pendingTestTasks_.begin() + static_cast<std::ptrdiff_t>(i));
        return static_cast<int>(i);
    }

    // Returns false only when the owning worker's ingress ring is full (the ref
    // is then still owned by the caller, who holds it for retry).
    [[gnu::cold]] [[gnu::noinline]] bool dispatchOwnerThreadTestTask_(void* ref) {
        const OwnerThreadTestTask& task = ownerThreadTestTaskOf(ref);
        if (task.target() == OwnerThreadTestTask::Target::ControlThread) {
            runOwnerThreadTestTask(ref, nullptr);
            return true;
        }
        const int32_t tid = task.topicId();
        const int32_t pid = task.partition();
        rune::replication::PartitionReplica* replica =
            registry_ ? registry_->lookup(tid, pid) : nullptr;
        if (workerPool_ && tid >= 0 && replica) {
            WorkerOp op;
            op.type      = WorkerOp::Type::TestTask;
            op.topicId   = tid;
            op.partition = pid;
            op.replica   = replica;
            op.egressPub = ref;   // opaque task ref (WorkerOp::TestTask contract)
            return static_cast<bool>(workerPool_->tryEnqueueOp(tid, pid, std::move(op)));
        }
        runOwnerThreadTestTask(ref, replica);   // control-thread-owned (or missing)
        return true;
    }

    static constexpr int32_t kTestTaskMsgTypeId = 0x7E57;  // F1 test-task ring msg
    // test thread(s) → control thread (many-to-one, F9a: replaces the SPSC ring
    // + BrokerNode's test-side mutex).  Small: tests enqueue one task at a time.
    rune::consensus::ControlRing<4096>                 testTaskRing_;
    // Control-thread-only: refs whose owning worker's ingress was full.
    std::vector<void*>                                 pendingTestTasks_;

    // Concrete consensus agent + construction-time tag (Task 11 fix 1).
    struct ConsensusRef {
        ConsensusKind                    kind    = ConsensusKind::Generic;
        rune::consensus::ConsensusAgent* single  = nullptr;
        NodeConsensusAgent*              multi   = nullptr;
        rune::consensus::Agent*          generic = nullptr;
        static ConsensusRef of(rune::consensus::Agent& a) noexcept {
            ConsensusRef r;
            if (auto* m = dynamic_cast<NodeConsensusAgent*>(&a)) { r.kind = ConsensusKind::Multi; r.multi = m; }
            else if (auto* c = dynamic_cast<rune::consensus::ConsensusAgent*>(&a)) { r.kind = ConsensusKind::Single; r.single = c; }
            else r.generic = &a;
            return r;
        }
        int doWork(int64_t nowNs) const {
            if (kind == ConsensusKind::Multi) [[likely]] return multi->doWork(nowNs);    // final → direct
            if (kind == ConsensusKind::Single) return single->doWork(nowNs);             // final → direct
            return generic->doWork(nowNs);                                               // test doubles only
        }
    };
    static_assert(std::is_final_v<NodeConsensusAgent> && std::is_final_v<rune::consensus::ConsensusAgent>,
                  "the runtime's consensus call must be devirtualised (final classes)");
    ConsensusRef                                       consensus_;
    rune::consensus::Election*                         election_;   // non-owning; null when no bridge
    // Allocation-free eligible-set tracking for bidirectional leadership rebalance.
    // Fixed stack buffers reused every duty cycle (no per-cycle heap); a heap vector
    // is built only on an actual eligible-set change (rare).  Cap bounds cluster size.
    static constexpr std::size_t                       kMaxClusterNodes = 64;
    std::array<rune::metadata::NodeId, kMaxClusterNodes> eligBuf_{};      // this cycle's eligible ids
    std::array<rune::metadata::NodeId, kMaxClusterNodes> prevEligBuf_{};  // last set that drove a rebalance
    std::size_t                                       prevEligLen_  = 0;
    bool                                              prevEligInit_ = false;
    // TEMP control-plane diagnostics (RUNE_CTRL_LOG): last-seen consensus
    // leader/term so we log only on change.  Remove once churn is fixed.
    int32_t                                           dbgLastLeader_ = -2;
    int64_t                                           dbgLastTerm_   = -2;
    // Reused every cycle for the idempotent convergence reassign (capacity persists
    // after warmup → no steady-state heap alloc).
    std::vector<rune::metadata::NodeId>               eligibleScratch_{};
    ReplicationMetadataBridge*                         bridge_;     // non-owning; null-safe
    rune::replication::AeronPartitionTransport*        transport_;  // non-owning; null-safe
    ReplicaRegistry*                                   registry_;   // non-owning; null-safe
    rune::gateway::ClientGateway*                      gateway_;    // non-owning; null-safe
    GroupCoordinationService*                          groupService_;    // non-owning; null-safe (CG-3)
    TransactionCoordinationService*                    txnService_;      // non-owning; null-safe (TX-3)
    MetadataService*                                   metadataService_; // non-owning; null-safe (MD1)
    WorkerPool*                                        workerPool_;      // non-owning; null = pre-E1 path
    // Task 8a: admin control-plane servicing hook (RULING-2 B). Null = no admin plane.
    AdminControlPlane*                                 adminControl_ = nullptr;
    int32_t                                            selfId_       = -1;
    // The same predicate as BrokerNode::isLeader() (one shared function).
    [[nodiscard]] bool isLocalLeader_() const noexcept {
        return isLocalLeader(consensus_.kind == ConsensusKind::Multi ? consensus_.multi : nullptr,
                             election_, selfId_);
    }
    // RUNE_CTRL_LOG temp diagnostic — read once at construction (not per scan).
    const bool                                         ctrlLog_ = ::getenv("RUNE_CTRL_LOG") != nullptr;
    LeaderObserver*                                    leaderObserver_ = nullptr;  // F9a, non-owning
    ControlledShutdown                                 controlledShutdown_;        // control thread (+ rings)
    StorageFailureHook                                 storageFailureHook_;
    std::function<void(const std::string&)>            fatalHook_;

    // Task 10 round 4: cleaner view publication (control-thread side).
    std::unique_ptr<CleanerViewChannel>                cleanerChannel_;
    std::vector<rune::replication::PartitionReplica*>  cleanerSnapshot_;   // generation being published
    std::uint64_t cleanerPublishingGen_ = static_cast<std::uint64_t>(-1);
    std::size_t   cleanerNextIndex_     = 0;
    bool          cleanerPublished_     = true;
    std::uint64_t cleanerAckedGen_      = 0;
    std::vector<uint8_t>                               cleanerScratch_;
    int publishCleanerView_() {
        if (!cleanerChannel_) return 0;
        int work = cleanerChannel_->ack.drain([this](int32_t, aeron::concurrent::AtomicBuffer& buf,
                                                     aeron::util::index_t off, aeron::util::index_t) {
            std::uint64_t g;
            std::memcpy(&g, buf.buffer() + off, 8);
            if (g > cleanerAckedGen_) cleanerAckedGen_ = g;
        }, 16);
        const std::uint64_t gen = registry_->generation();
        if (gen != cleanerPublishingGen_) {   // cold: on registry change only
            cleanerSnapshot_      = registry_->allReplicas();
            cleanerPublishingGen_ = gen;
            cleanerNextIndex_     = 0;
            cleanerPublished_     = false;
        }
        while (!cleanerPublished_) {
            const std::size_t total = cleanerSnapshot_.size();
            const std::size_t count = std::min<std::size_t>(CleanerViewChannel::kPtrsPerChunk,
                                                            total - cleanerNextIndex_);
            cleanerScratch_.resize(CleanerViewChannel::kHeaderBytes + 8 * count);
            uint8_t* p = cleanerScratch_.data();
            const uint32_t t32 = static_cast<uint32_t>(total), i32 = static_cast<uint32_t>(cleanerNextIndex_),
                           c32 = static_cast<uint32_t>(count), pad = 0;
            std::memcpy(p, &gen, 8); std::memcpy(p + 8, &t32, 4); std::memcpy(p + 12, &i32, 4);
            std::memcpy(p + 16, &c32, 4); std::memcpy(p + 20, &pad, 4);
            for (std::size_t i = 0; i < count; ++i) {
                const std::uint64_t v = reinterpret_cast<std::uintptr_t>(cleanerSnapshot_[cleanerNextIndex_ + i]);
                std::memcpy(p + CleanerViewChannel::kHeaderBytes + 8 * i, &v, 8);
            }
            if (!cleanerChannel_->view.write(p, cleanerScratch_.size(), CleanerViewChannel::kViewMsgType))
                break;   // ring full: resumed next cycle (time-independent)
            cleanerNextIndex_ += count;
            ++work;
            if (cleanerNextIndex_ >= total) cleanerPublished_ = true;
        }
        return work;
    }
    rune::consensus::SlowTick                          failoverTick_;
    std::uint64_t                                      cycles_                = 0;
    std::uint64_t                                      routingImagePublishes_ = 0;
    // routingVersion() of the image last published; UINT64_MAX = none yet
    // (routingVersion() starts at 0, so the first cycle always builds).
    std::uint64_t                                      routingVersionBuilt_   = UINT64_MAX;
    bool                                               fatalReported_ = false;
    bool                                               storageFailureReported_ = false;
    void checkReplicaStorage_(rune::replication::PartitionReplica* replica) {
        if (!replica->isDiskBacked()) return;
        rune::storage::Log* log = replica->log();
        if (!log->storageFailed()) [[likely]] return;
        if (storageFailureReported_) return;
        storageFailureReported_ = true;
        spdlog::critical("NodeRuntimeAgent: fsync FAILED on {} (errno={}) — halting the broker",
                         log->dir(), log->storageErrno());
        if (storageFailureHook_) storageFailureHook_(log->dir(), log->storageErrno());
    }
};

} // namespace rune::node
