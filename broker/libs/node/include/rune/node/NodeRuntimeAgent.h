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
#include <rune/gateway/ClientGateway.h>
#include <rune/node/GroupCoordinationService.h>
#include <rune/node/TransactionCoordinationService.h>
#include <rune/node/MetadataService.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/node/WorkerPool.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/OffsetMap.h>
#include <rune/consensus/Election.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/Gpid.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
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
        : consensus_(consensusAgent)
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
        : consensus_(consensusAgent)
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
    // Agent::doWork — single-writer duty cycle.
    // Returns sum of work done by all components.
    // -----------------------------------------------------------------------
    const char* name() const noexcept override { return "rune-runtime"; }

    int doWork(int64_t nowNs) override {
        // 1. Consensus (Election + IngressRing drain).
        int work = consensus_.doWork(nowNs);

        // 2. Metadata bridge — detect leader/epoch change and flip replica roles.
        //    Reads election_ AFTER consensus step so the state is current.
        if (bridge_ && election_) {
            const rune::consensus::NodeId  lid   = election_->leaderMemberId();
            const rune::replication::Epoch epoch =
                static_cast<rune::replication::Epoch>(election_->leadershipTermId());
            work += bridge_->doWork(lid, epoch);
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
            if (election_) {
                // FAILOVER liveness window (2026-09-24 bugfix): reassignment must
                // NOT use the short consensus heartbeat window (max(electionTimeout,
                // 3*statusInterval) — only ~300ms with the e2e config).  Right after
                // election a live follower can go >300ms without an AppendPosition,
                // so the raw consensus eligible set FLAPS ([0,1,2]→[0,1]→[1]) and the
                // death-edge gate below fires on that jitter — reassigning ALL
                // leadership to one node, collapsing ISR and timing out produce on
                // multi-partition topics.  Use a window 3x the heartbeat timeout:
                // long enough that a live follower's transient post-election gap
                // (~1 heartbeat) never reads as death, short enough that a genuinely
                // dead node is reassigned within ~3 heartbeats (fast failover kept).
                const int64_t failoverTimeoutNs =
                    3 * election_->leaderHeartbeatTimeoutNs();
                const std::vector<rune::metadata::NodeId> eligible =
                    election_->eligibleNodeIds(nowNs, failoverTimeoutNs);
                // REASSIGN-GATE (2026-09-24 bugfix): only reassign on an actual
                // node-DEATH edge — a node that WAS eligible last cycle is no
                // longer. Running reassignDeadPartitionLeaders() every duty cycle
                // caused non-convergent churn (63k/run) on a HEALTHY cluster,
                // collapsing replica sets to RF=1 → ISR stuck at 1 → acks=all
                // MinIsrViolation. On a stable/growing eligible set we do nothing.
                bool aNodeDied = false;
                for (auto id : prevEligible_) {
                    if (std::find(eligible.begin(), eligible.end(), id) == eligible.end()) {
                        aNodeDied = true;
                        break;
                    }
                }
                if (aNodeDied) {
                    std::string es; for (auto id : eligible) es += std::to_string(id) + ",";
                    std::string ps; for (auto id : prevEligible_) ps += std::to_string(id) + ",";
                    spdlog::info("REASSIGN-GATE: node-death edge, prevEligible=[{}] eligible=[{}] -> reassigning", ps, es);
                    work += static_cast<int>(
                        metadataService_->reassignDeadPartitionLeaders(eligible));
                }
                prevEligible_ = eligible;
            }

            // Rebuild RoutingImage and push to gateway each cycle.
            // This is cheap (shared_ptr swap + array copy) and done on the
            // duty-cycle thread — no cross-thread synchronisation needed.
            // The image is captured by the gateway's doWork on the SAME thread,
            // so there is no cross-thread race on routingImage_.
            //
            // BOOTSTRAP FIX (MD1-13): publish a RoutingImage on every duty cycle.
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
            if (gateway_) {
                const auto& cmap = metadataService_->committedMap();
                if (!cmap.empty()) {
                    // Steady state: use the committed assignment map.
                    auto img = rune::metadata::RoutingImage::buildFromMap(
                        cmap, rune::metadata::gpidOf);
                    gateway_->setRoutingImageSnapshot(std::move(img));
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
                    }
                }
            }
        } else if (registry_) {
            // Pre-E1 path: iterate registry directly (all replicas).
            for (auto* replica : registry_->allReplicas()) {
                if (replica) work += replica->doWork(nowNs);
            }
        }

        // 6. Gateway.
        if (gateway_) {
            work += gateway_->doWork(nowNs);
        }

        // 6.5. CP-6: Drain pending cleaner swaps from LogCleanerAgent.
        //      LogCleanerAgent runs on its own thread and enqueues compacted
        //      segment bases via Log::enqueueCleanedSegment().  The actual
        //      in-memory segment-map reload must happen on this single-writer
        //      thread (not on the cleaner thread), so we drain them here.
        //
        //      CP-9: Also drain retention deletions (Log::drainRetentionDeletes).
        //      The cleaner enqueues bases of segments renamed to .deleted via
        //      Log::enqueueDeletedSegment().  The segments_ erase MUST happen
        //      here on the append thread (single-writer invariant).
        //
        //      E1/E2: when workerPool_ is wired:
        //        - INTERNAL replicas: drained here (control thread owns them).
        //        - USER replicas: Worker::doWork already handles these drains
        //          for each owned shard. Skip them here to avoid double-draining.
        if (workerPool_ && registry_) {
            for (auto* replica : registry_->allReplicas()) {
                // Only internal replicas — user replicas are drained by workers.
                if (replica && replica->topicId() < 0 && replica->isDiskBacked()) {
                    work += replica->log()->drainCleanerSwaps();
                    work += replica->log()->drainRetentionDeletes();
                    work += replica->log()->drainStagingRenamesDone();
                }
            }
        } else if (!workerPool_ && registry_) {
            for (auto* replica : registry_->allReplicas()) {
                // Task M1 precondition #2: skip memory-backed replicas.
                // drainCleanerSwaps/drainRetentionDeletes/drainStagingRenamesDone are
                // disk-Log-only operations; replica->log() asserts isDiskBacked().
                if (replica && replica->isDiskBacked()) {
                    work += replica->log()->drainCleanerSwaps();
                    work += replica->log()->drainRetentionDeletes();
                    // Append-p99 fix: apply completed staging→base renames the
                    // background (cleaner) thread performed, so logPath_ tracks the
                    // on-disk name (append-thread-only segments_ access).
                    work += replica->log()->drainStagingRenamesDone();
                }
            }
        }

        return work;
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
                    const int deleted = log->deleteOldSegments(retBytes);
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
                        work += log->deleteOldSegments(retBytes);
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
                    cleaner.cleanSegment(*log,
                                         /*segBase=*/segToCompact,
                                         /*segEnd=*/segEnd,
                                         /*globalDirtyStart=*/firstDirty,
                                         nowNs,
                                         replica->deleteRetentionMs());

                    // 8. Advance the cleaned watermark.
                    replica->setLastCleanOffset(segEnd);

                    ++work;
                    compactionRan = true;
                }
            }

            // ---------------------------------------------------------------
            // CP-3: CompactDelete — after compaction, run size retention.
            // Ordering: compact FIRST (already done above), THEN delete old segments.
            // ---------------------------------------------------------------
            if (policy == rune::replication::CleanupPolicy::CompactDelete) {
                const int64_t retBytes = replica->retentionBytes();
                if (retBytes >= 0) {
                    const int deleted = log->deleteOldSegments(retBytes);
                    work += deleted;
                }
            }
            // Suppress unused-variable warning when compactionRan is not used below.
            (void)compactionRan;
        }
        return work;
    }

private:
    rune::consensus::Agent&                            consensus_;
    rune::consensus::Election*                         election_;   // non-owning; null when no bridge
    std::vector<rune::metadata::NodeId>                prevEligible_; // REASSIGN-GATE: last cycle's eligible set (death-edge detection)
    ReplicationMetadataBridge*                         bridge_;     // non-owning; null-safe
    rune::replication::AeronPartitionTransport*        transport_;  // non-owning; null-safe
    ReplicaRegistry*                                   registry_;   // non-owning; null-safe
    rune::gateway::ClientGateway*                      gateway_;    // non-owning; null-safe
    GroupCoordinationService*                          groupService_;    // non-owning; null-safe (CG-3)
    TransactionCoordinationService*                    txnService_;      // non-owning; null-safe (TX-3)
    MetadataService*                                   metadataService_; // non-owning; null-safe (MD1)
    WorkerPool*                                        workerPool_;      // non-owning; null = pre-E1 path
};

} // namespace rune::node
