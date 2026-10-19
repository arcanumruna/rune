#pragma once
// SecurityPartition.h — the `__security` partition (authentication spec §3, plan D39).
//
// Exactly `__catalog`'s design (CatalogPartition.h), restricted to brokers and security admins: one partition,
// replicated to every broker, acks=all, minIsr = majority, never compacted / retention-deleted, always disk-backed;
// the replica, its disk Log and the SecurityApplier are owned and driven by the ADMIN thread; its leadership comes
// from the controller-ISR path (single node: the consensus leader); it rides the admin replication plane next to
// `__catalog`, frames dispatched by topic id (AdminLogPartition.h).

#include <rune/node/AdminLogPartition.h>
#include <rune/protocol/client/Protocol.h>

namespace rune::node {

/// `__security`'s topic id (== rune::protocol::client::kSecurityTopicId).  Partition 0 is its only partition.
inline constexpr int32_t kSecurityTopicId   = kAdminLogSecurityTopicId;   // -7
inline constexpr int32_t kSecurityPartition = kAdminLogPartition;
static_assert(kSecurityTopicId == rune::protocol::client::kSecurityTopicId,
              "the broker's __security topic id must equal the wire's");

[[nodiscard]] inline int32_t securityMinIsr(std::size_t brokers) noexcept { return adminLogMinIsr(brokers); }

/// The `__security` PartitionConfig (makeAdminLogPartitionConfig for -7).
[[nodiscard]] inline rune::replication::PartitionConfig makeSecurityPartitionConfig(const NodeConfig& config,
                                                                                    std::size_t brokers) {
    return makeAdminLogPartitionConfig(config, brokers, kSecurityTopicId);
}

} // namespace rune::node
