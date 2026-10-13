#pragma once
// CatalogPartition.h — the `__catalog` partition (catalog history spec §3, §5.6).
//
// "One partition, replicated to every broker.  Written with acks=all; minIsr is
//  a majority.  Unclean leader election is disabled.  Never compacted, never
//  retention-deleted.  Always disk-backed.  It is never transient.  It is the
//  single source of truth for the catalog."
//
// Ownership (RULING-2 A, hard rules 1/2): the catalog PartitionReplica, its
// disk Log and its replication plane are owned and driven by the ADMIN thread
// only (AdminWorker::doWork).  The replica is never in the control thread's
// ReplicaRegistry.  Its leadership comes from the controller-ISR path
// (MetadataService::decideCoordinatorPartition, ruling 7): the bridge relays the
// committed (-6, 0) assignment on the control→admin role ring, and the replica's
// ISR proposals (AlterIsr) travel admin→control on the admin→control ring
// (AdminIsrChannel) to the same controller channel the coordinator partitions
// use.  Single node: the consensus leader drives it (no controller path).
//
// This header stays free of NodeConfig.h (the bridge includes it; NodeConfig's
// rune::node::TopicSpec collides with rune::metadata::TopicSpec in tests).

#include <rune/replication/PartitionReplica.h>   // PartitionConfig, PartitionReplica
#include <rune/replication/ReplicationMessages.h>

#include <cstddef>
#include <cstdint>
#include <exception>

#include <spdlog/spdlog.h>

namespace rune::node {

struct NodeConfig;

/// The catalog's topic id (== rune::protocol::client::kCatalogTopicId; pinned by
/// a static_assert in CatalogPartition.cpp).  Partition 0 is its only partition.
inline constexpr int32_t kCatalogTopicId   = -6;
inline constexpr int32_t kCatalogPartition = 0;

/// minIsr of `__catalog`: a majority of the brokers.
[[nodiscard]] inline int32_t catalogMinIsr(std::size_t brokers) noexcept {
    return static_cast<int32_t>(brokers / 2 + 1);
}

/// The `__catalog` PartitionConfig: Delete policy with no retention (never
/// compacted, never retention-deleted, log start 0), minIsr = majority,
/// Majority durability.  Every catalog append is acks=all (the replica's
/// min-ISR gate: PartitionReplica::append(records, /*acksAll=*/true)).
[[nodiscard]] rune::replication::PartitionConfig makeCatalogPartitionConfig(const NodeConfig& config,
                                                                            std::size_t brokers);

/// Frames the admin thread's catalog plane polled that it could not apply: a
/// frame for another (topic, partition) — never possible on the dedicated
/// catalog stream unless a peer is misconfigured — or a replica handler that
/// threw.  Both are counted and logged (never silent).
struct CatalogFrameStats {
    uint64_t misrouted = 0;
    uint64_t failed    = 0;
};

/// Apply one replication frame received on the catalog plane to the catalog
/// replica (admin thread).  Mirrors Worker::pollReplTransportDispatch_ for the
/// single admin-owned replica: the frame pointer is a zero-copy view valid only
/// inside this call (the replica copies what it keeps).
inline void dispatchCatalogFrame(rune::replication::PartitionReplica& replica,
                                 int32_t topicId, int32_t partition, uint16_t templateId,
                                 const uint8_t* frame, std::size_t len, int64_t nowNs,
                                 CatalogFrameStats& stats) noexcept {
    namespace rr = rune::replication;
    if (topicId != kCatalogTopicId || partition != kCatalogPartition) [[unlikely]] {
        if (stats.misrouted++ % 1024 == 0)
            spdlog::error("__catalog plane: frame for topicId={} partition={} (templateId={}) dropped — "
                          "the catalog stream carries only (-6, 0); a peer is misconfigured "
                          "(RUNE_CATALOG_REPL_STREAM_ID collision?) — dropped so far: {}",
                          topicId, partition, templateId, stats.misrouted);
        return;
    }
    try {
        switch (templateId) {
        case static_cast<uint16_t>(rr::ReplicationTemplateId::FetchRequest):
            replica.onFetchRequest(rr::FetchRequestMsg::decode(frame, 0), nowNs);
            break;
        case static_cast<uint16_t>(rr::ReplicationTemplateId::FetchResponse):
            replica.applyReplicated(rr::FetchResponseMsg::decode(frame, 0, len));
            break;
        case static_cast<uint16_t>(rr::ReplicationTemplateId::OffsetsForLeaderEpochReq):
            replica.onOffsetsForLeaderEpochRequest(rr::OffsetsForLeaderEpochReqMsg::decode(frame, 0));
            break;
        case static_cast<uint16_t>(rr::ReplicationTemplateId::OffsetsForLeaderEpochResp):
            replica.applyOffsetsForLeaderEpochResponse(rr::OffsetsForLeaderEpochRespMsg::decode(frame, 0));
            break;
        case rr::kReconcileFetchReqTemplateId:
            replica.onReconcileRequest(rr::FetchRequestMsg::decode(frame, 0));
            break;
        case rr::kReconcileFetchRespTemplateId:
            replica.applyReconcileResponse(rr::FetchResponseMsg::decode(frame, 0, len));
            break;
        default:
            // The transport already drops unknown template ids before dispatch.
            break;
        }
    } catch (const std::exception& ex) {
        if (stats.failed++ % 1024 == 0)
            spdlog::error("__catalog plane: templateId={} handler threw: {} (failures so far: {})",
                          templateId, ex.what(), stats.failed);
    } catch (...) {
        if (stats.failed++ % 1024 == 0)
            spdlog::error("__catalog plane: templateId={} handler threw an unknown exception "
                          "(failures so far: {})", templateId, stats.failed);
    }
}

} // namespace rune::node
