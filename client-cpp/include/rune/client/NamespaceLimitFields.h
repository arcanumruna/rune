#pragma once
// NamespaceLimitFields.h — the 29 mask-addressable NamespaceLimits fields (spec §5.2, plan D13) by index, with their
// kebab-case names, shared by the client API (NamespaceLimitsPatch) and the CLI (`rune namespace create --<name> N`,
// `rune namespace limits <ns> --set <name>=N`).  Index i is presentMask bit i: 28 u64 fields in declaration order,
// then featureBits (u32).  Control plane only.

#include <rune/protocol/NamespaceLimits.h>
#include <rune/protocol/client/NamespaceMessages.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string_view>

namespace rune::client {

struct NamespaceLimitField {
    std::string_view name;                                // kebab-case (CLI flag without "--")
    uint64_t rune::protocol::NamespaceLimits::* u64;      // nullptr for featureBits (index 28, u32)
};

using rune::protocol::NamespaceLimits;
inline constexpr std::array<NamespaceLimitField, rune::protocol::kNamespaceLimitFieldCount> kNamespaceLimitFields{{
    {"max-topics", &NamespaceLimits::maxTopics},
    {"max-partitions-per-topic", &NamespaceLimits::maxPartitionsPerTopic},
    {"max-partitions-total", &NamespaceLimits::maxPartitionsTotal},
    {"max-views", &NamespaceLimits::maxViews},
    {"max-schemas", &NamespaceLimits::maxSchemas},
    {"max-versions-per-schema", &NamespaceLimits::maxVersionsPerSchema},
    {"max-groups", &NamespaceLimits::maxGroups},
    {"max-users", &NamespaceLimits::maxUsers},
    {"max-disk-bytes", &NamespaceLimits::maxDiskBytes},
    {"max-retention-ms", &NamespaceLimits::maxRetentionMs},
    {"max-retention-bytes", &NamespaceLimits::maxRetentionBytes},
    {"max-message-bytes", &NamespaceLimits::maxMessageBytes},
    {"max-batch-bytes", &NamespaceLimits::maxBatchBytes},
    {"produce-bytes-per-sec", &NamespaceLimits::produceBytesPerSec},
    {"produce-records-per-sec", &NamespaceLimits::produceRecordsPerSec},
    {"fetch-bytes-per-sec", &NamespaceLimits::fetchBytesPerSec},
    {"request-ops-per-sec", &NamespaceLimits::requestOpsPerSec},
    {"query-cycles-per-sec", &NamespaceLimits::queryCyclesPerSec},
    {"max-connections", &NamespaceLimits::maxConnections},
    {"max-connections-per-user", &NamespaceLimits::maxConnectionsPerUser},
    {"max-sessions-per-user", &NamespaceLimits::maxSessionsPerUser},
    {"max-open-txns", &NamespaceLimits::maxOpenTxns},
    {"max-txn-time-ms", &NamespaceLimits::maxTxnTimeMs},
    {"max-view-memory-bytes", &NamespaceLimits::maxViewMemoryBytes},
    {"min-replication-factor", &NamespaceLimits::minReplicationFactor},
    {"max-replication-factor", &NamespaceLimits::maxReplicationFactor},
    {"lowest-acks", &NamespaceLimits::lowestAcks},
    {"lowest-min-insync", &NamespaceLimits::lowestMinInsync},
    {"feature-bits", nullptr},
}};
inline constexpr std::size_t kNamespaceFeatureBitsField = 28;

/// The index of the field named `name` (kebab-case), or nullopt.
[[nodiscard]] constexpr std::optional<std::size_t> namespaceLimitIndex(std::string_view name) noexcept {
    for (std::size_t i = 0; i < kNamespaceLimitFields.size(); ++i)
        if (kNamespaceLimitFields[i].name == name) return i;
    return std::nullopt;
}
[[nodiscard]] constexpr uint64_t namespaceLimitGet(const NamespaceLimits& l, std::size_t i) noexcept {
    return i == kNamespaceFeatureBitsField ? l.featureBits : l.*(kNamespaceLimitFields[i].u64);
}
/// false (nothing changed) when `v` does not fit the field (featureBits is u32) or `i` is out of range.
[[nodiscard]] constexpr bool namespaceLimitSet(NamespaceLimits& l, std::size_t i, uint64_t v) noexcept {
    if (i >= kNamespaceLimitFields.size()) return false;
    if (i == kNamespaceFeatureBitsField) {
        if (v > std::numeric_limits<uint32_t>::max()) return false;
        l.featureBits = static_cast<uint32_t>(v);
        return true;
    }
    l.*(kNamespaceLimitFields[i].u64) = v;
    return true;
}

/// createNamespace(): the limits the request names (presentMask bit i); every other field takes the broker's current
/// default (RUNE_NS_DEFAULT_*) when the namespace is created.
struct NamespaceLimitsPatch {
    NamespaceLimits values{};
    std::array<uint8_t, rune::protocol::client::kNamespacePresentMaskBytes> present{};
    /// false when `v` does not fit the field or `i` is out of range.
    [[nodiscard]] constexpr bool set(std::size_t i, uint64_t v) noexcept {
        if (!namespaceLimitSet(values, i, v)) return false;
        present[i >> 3] = static_cast<uint8_t>(present[i >> 3] | (1u << (i & 7u)));
        return true;
    }
    [[nodiscard]] constexpr bool has(std::size_t i) const noexcept {
        return rune::protocol::client::namespaceMaskHas(present, i);
    }
};

/// Every one of the 29 fields present (setNamespaceLimits sends the FULL record).
inline constexpr std::array<uint8_t, rune::protocol::client::kNamespacePresentMaskBytes> kNamespaceFullMask{
    0xFF, 0xFF, 0xFF, 0x1F};

} // namespace rune::client
