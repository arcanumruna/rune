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
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/LogCleaner.h>
#include <rune/storage/OffsetMap.h>
#include <rune/consensus/Election.h>

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
    // @param txnService      TransactionCoordinationService pointer (may be null). Non-owning.
    NodeRuntimeAgent(rune::consensus::Agent&            consensusAgent,
                     rune::consensus::Election*          election,
                     ReplicationMetadataBridge*          bridge,
                     rune::replication::AeronPartitionTransport* transport,
                     ReplicaRegistry*                    registry,
                     rune::gateway::ClientGateway*       gateway,
                     GroupCoordinationService*           groupService = nullptr,
                     TransactionCoordinationService*     txnService   = nullptr) noexcept
        : consensus_(consensusAgent)
        , election_(election)
        , bridge_(bridge)
        , transport_(transport)
        , registry_(registry)
        , gateway_(gateway)
        , groupService_(groupService)
        , txnService_(txnService)
    {}

    // Backward-compatible constructor for single-node / tests without replication.
    NodeRuntimeAgent(rune::consensus::Agent&        consensusAgent,
                     rune::gateway::ClientGateway*  gateway,
                     GroupCoordinationService*      groupService = nullptr,
                     TransactionCoordinationService* txnService  = nullptr) noexcept
        : consensus_(consensusAgent)
        , election_(nullptr)
        , bridge_(nullptr)
        , transport_(nullptr)
        , registry_(nullptr)
        , gateway_(gateway)
        , groupService_(groupService)
        , txnService_(txnService)
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

        // 6.5. Bounded compaction pass — leader-only, sealed segments only.
        //      Samples nowNs once per cycle (already done above).
        work += runBoundedCompaction(nowNs);

        return work;
    }

    // -----------------------------------------------------------------------
    // runBoundedCompaction — CP-2: bounded, leader-only compaction pass.
    //
    // Iterates all replicas in the registry.  For each replica:
    //   1. Skip if the replica's role is not Leader.
    //   2. Skip if the cleanup policy is not Compact (or CompactDelete).
    //   3. Compute dirty ratio = (LEO - firstDirty) / LEO; skip if below threshold.
    //      (Conservative: treat firstDirty == 0 for now — always compact if dirty.)
    //   4. Determine the sealed range: [0, activeSegmentBase).  If there are no
    //      sealed segments (only one segment), skip.
    //   5. Call LogCleaner::clean([0, activeBase), nowNs, deleteRetentionMs=1 day).
    //
    // Returns number of compaction passes run (>0 if any work done).
    //
    // Single-writer invariant: called on the AgentRunner duty-cycle thread only.
    // NEVER touches the active segment (cleanBoundaryOffset == activeBase).
    // -----------------------------------------------------------------------
    int runBoundedCompaction(int64_t nowNs) {
        if (!registry_) return 0;

        int work = 0;
        for (auto* replica : registry_->allReplicas()) {
            if (!replica) continue;

            // 1. Leader-only.
            if (replica->role() != rune::replication::Role::Leader) continue;

            // 2. Compact policy only.
            const auto policy = replica->cleanupPolicy();
            if (policy == rune::replication::CleanupPolicy::Delete) continue;

            // 3. Need a log.
            rune::storage::Log* log = replica->log();
            if (!log) continue;

            // 4. Must have at least 2 segments (one sealed, one active).
            const auto bases = log->segmentBaseOffsets();
            if (bases.size() < 2) continue;

            // The active segment is the LAST one (highest base offset).
            const int64_t activeBase = bases.back();

            // 5. Run LogCleaner::clean over the sealed range [0, activeBase).
            //    cleanBoundaryOffset == activeBase ensures the active segment
            //    is NEVER touched (records at offset >= activeBase are left intact).
            //
            // deleteRetentionMs: use 1 day default (86_400_000 ms).
            // Bounded: one clean() call per partition per cycle.
            rune::storage::LogCleaner cleaner;
            cleaner.clean(*log, /*firstDirtyOffset=*/0, /*cleanBoundaryOffset=*/activeBase,
                          nowNs, /*deleteRetentionMs=*/86'400'000LL);
            log->reloadSegments();

            ++work;
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
    GroupCoordinationService*                          groupService_; // non-owning; null-safe (CG-3)
    TransactionCoordinationService*                    txnService_;   // non-owning; null-safe (TX-3)
};

} // namespace rune::node
