#pragma once
// AdminLogPartition.h — the helpers every ADMIN-owned replicated log shares (authentication plan D39): `__catalog`
// (-6) and `__security` (-7).  Both are one partition, replicated to every broker, acks=all, minIsr = majority,
// never compacted / retention-deleted, always disk-backed, owned and driven by the admin thread, riding the admin
// replication plane (RUNE_CATALOG_REPL_STREAM_ID) — the plane's frames are dispatched to the replica by topic id.
// The catalog-named helpers (CatalogPartition.h) delegate here.
//
// Free of NodeConfig.h (the bridge includes the catalog header; NodeConfig's rune::node::TopicSpec collides with
// rune::metadata::TopicSpec in tests).

#include <rune/replication/PartitionReplica.h>   // PartitionConfig, PartitionReplica
#include <rune/replication/ReplicationMessages.h>

#include <cstddef>
#include <cstdint>
#include <exception>

#include <spdlog/spdlog.h>

namespace rune::node {

struct NodeConfig;

/// The admin logs' topic ids (== rune::protocol::client::k{Catalog,Security}TopicId; pinned by static_asserts in
/// CatalogPartition.cpp / SecurityPartition.h).  Partition 0 is each one's only partition.
inline constexpr int32_t kAdminLogCatalogTopicId  = -6;
inline constexpr int32_t kAdminLogSecurityTopicId = -7;
inline constexpr int32_t kAdminLogPartition       = 0;

/// minIsr of an admin log: a majority of the brokers.
[[nodiscard]] inline int32_t adminLogMinIsr(std::size_t brokers) noexcept {
    return static_cast<int32_t>(brokers / 2 + 1);
}

/// The PartitionConfig of the admin log `topicId`: Delete policy with no retention (never compacted, never
/// retention-deleted, log start 0), minIsr = majority, Majority durability.  Every append is acks=all (the replica's
/// min-ISR gate: PartitionReplica::append(records, /*acksAll=*/true)).
[[nodiscard]] rune::replication::PartitionConfig makeAdminLogPartitionConfig(const NodeConfig& config,
                                                                             std::size_t brokers,
                                                                             int32_t topicId);

/// Frames the admin thread's plane polled that it could not apply: a frame for a (topic, partition) it owns no
/// replica for — never possible on the dedicated admin stream unless a peer is misconfigured — or a replica handler
/// that threw.  Both are counted and logged (never silent).
struct AdminLogFrameStats {
    uint64_t misrouted = 0;
    uint64_t failed    = 0;
};

/// The admin thread's replicas by topic id: -6 -> catalog, -7 -> security (either may be null: no such log).
struct AdminLogReplicas {
    rune::replication::PartitionReplica* catalog  = nullptr;
    rune::replication::PartitionReplica* security = nullptr;
    [[nodiscard]] rune::replication::PartitionReplica* forTopic(int32_t topicId) const noexcept {
        return topicId == kAdminLogCatalogTopicId ? catalog : topicId == kAdminLogSecurityTopicId ? security : nullptr;
    }
};

/// Apply one replication frame received on the admin plane to the replica its topic id names (admin thread).
/// Mirrors Worker::pollReplTransportDispatch_ for the admin-owned replicas: the frame pointer is a zero-copy view
/// valid only inside this call (the replica copies what it keeps).
inline void dispatchAdminLogFrame(const AdminLogReplicas& replicas, int32_t topicId, int32_t partition,
                                  uint16_t templateId, const uint8_t* frame, std::size_t len, int64_t nowNs,
                                  AdminLogFrameStats& stats) noexcept {
    namespace rr = rune::replication;
    rr::PartitionReplica* replica = partition == kAdminLogPartition ? replicas.forTopic(topicId) : nullptr;
    if (replica == nullptr) [[unlikely]] {
        if (stats.misrouted++ % 1024 == 0)
            spdlog::error("admin plane: frame for topicId={} partition={} (templateId={}) dropped — the admin stream "
                          "carries only (-6, 0) and (-7, 0), and this broker has no replica for it; a peer is "
                          "misconfigured (RUNE_CATALOG_REPL_STREAM_ID collision?) — dropped so far: {}",
                          topicId, partition, templateId, stats.misrouted);
        return;
    }
    try {
        switch (templateId) {
        case static_cast<uint16_t>(rr::ReplicationTemplateId::FetchRequest):
            replica->onFetchRequest(rr::FetchRequestMsg::decode(frame, 0, len), nowNs);
            break;
        case static_cast<uint16_t>(rr::ReplicationTemplateId::FetchResponse):
            replica->applyReplicated(rr::FetchResponseMsg::decode(frame, 0, len));
            break;
        case static_cast<uint16_t>(rr::ReplicationTemplateId::OffsetsForLeaderEpochReq):
            replica->onOffsetsForLeaderEpochRequest(rr::OffsetsForLeaderEpochReqMsg::decode(frame, 0));
            break;
        case static_cast<uint16_t>(rr::ReplicationTemplateId::OffsetsForLeaderEpochResp):
            replica->applyOffsetsForLeaderEpochResponse(rr::OffsetsForLeaderEpochRespMsg::decode(frame, 0));
            break;
        case rr::kReconcileFetchReqTemplateId:
            replica->onReconcileRequest(rr::FetchRequestMsg::decode(frame, 0, len));
            break;
        case rr::kReconcileFetchRespTemplateId:
            replica->applyReconcileResponse(rr::FetchResponseMsg::decode(frame, 0, len));
            break;
        default:
            // The transport already drops unknown template ids before dispatch.
            break;
        }
    } catch (const std::exception& ex) {
        if (stats.failed++ % 1024 == 0)
            spdlog::error("admin plane: topicId={} templateId={} handler threw: {} (failures so far: {})",
                          topicId, templateId, ex.what(), stats.failed);
    } catch (...) {
        if (stats.failed++ % 1024 == 0)
            spdlog::error("admin plane: topicId={} templateId={} handler threw an unknown exception "
                          "(failures so far: {})", topicId, templateId, stats.failed);
    }
}

} // namespace rune::node
