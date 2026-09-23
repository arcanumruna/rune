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
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/OffsetMap.h>
#include <rune/consensus/Election.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/Gpid.h>

#include <cstdint>
#include <limits>

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
                     MetadataService*                    metadataService = nullptr) noexcept
        : consensus_(consensusAgent)
        , election_(election)
        , bridge_(bridge)
        , transport_(transport)
        , registry_(registry)
        , gateway_(gateway)
        , groupService_(groupService)
        , txnService_(txnService)
        , metadataService_(metadataService)
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
    {}

    NodeRuntimeAgent(const NodeRuntimeAgent&)            = delete;
    NodeRuntimeAgent& operator=(const NodeRuntimeAgent&) = delete;

    // -----------------------------------------------------------------------
    // Agent::doWork — single-writer duty cycle.
    // Returns sum of work done by all components.
    // -----------------------------------------------------------------------
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

            // Rebuild RoutingImage and push to gateway each cycle.
            // This is cheap (shared_ptr swap + array copy) and done on the
            // duty-cycle thread — no cross-thread synchronisation needed.
            // The image is captured by the gateway's doWork on the SAME thread,
            // so there is no cross-thread race on routingImage_.
            if (gateway_) {
                auto img = rune::metadata::RoutingImage::buildFromMap(
                    metadataService_->committedMap(),
                    rune::metadata::gpidOf);
                gateway_->setRoutingImageSnapshot(std::move(img));
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

        // 5. Per-replica duty cycle — ISR checks (leader) / fetch emission (follower).
        if (registry_) {
            for (auto* replica : registry_->allReplicas()) {
                if (replica) {
                    work += replica->doWork(nowNs);
                }
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
        if (registry_) {
            for (auto* replica : registry_->allReplicas()) {
                if (replica && replica->log()) {
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

            // 3. Need a log.
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
    ReplicationMetadataBridge*                         bridge_;     // non-owning; null-safe
    rune::replication::AeronPartitionTransport*        transport_;  // non-owning; null-safe
    ReplicaRegistry*                                   registry_;   // non-owning; null-safe
    rune::gateway::ClientGateway*                      gateway_;    // non-owning; null-safe
    GroupCoordinationService*                          groupService_;    // non-owning; null-safe (CG-3)
    TransactionCoordinationService*                    txnService_;      // non-owning; null-safe (TX-3)
    MetadataService*                                   metadataService_; // non-owning; null-safe (MD1)
};

} // namespace rune::node
