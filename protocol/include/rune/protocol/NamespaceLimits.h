#pragma once
// NamespaceLimits.h — the per-namespace limits record (spec §5.2, plan D13), ONE fixed little-endian block shared by
// the wire (NamespaceDdl 81 / ListNamespacesResponse 83), the codec (`__catalog` namespace events) and the client.
//
// Layout (256 bytes, no padding; the wire order IS the declaration order):
//   28 x u64   objects (8) | storage (5) | throughput (5) | connections (5) | views (1) | durability (4)
//              0 = unlimited where §5.2 allows it, otherwise the value
//   u32        featureBits (bit per feature, see kNsFeature*)
//   u32        reserved0   (placement / password policy / encryption key: zero in Phase 1)
//   3 x u64    reserved1..3 (carried as zero)
// The first 29 fields (28 u64 + featureBits) are addressable by a presence mask (NamespaceDdl: bit i = field i).

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace rune::protocol {

struct NamespaceLimits {
    // objects
    uint64_t maxTopics = 0, maxPartitionsPerTopic = 0, maxPartitionsTotal = 0, maxViews = 0, maxSchemas = 0,
             maxVersionsPerSchema = 0, maxGroups = 0, maxUsers = 0;
    // storage
    uint64_t maxDiskBytes = 0, maxRetentionMs = 0, maxRetentionBytes = 0, maxMessageBytes = 0, maxBatchBytes = 0;
    // throughput
    uint64_t produceBytesPerSec = 0, produceRecordsPerSec = 0, fetchBytesPerSec = 0, requestOpsPerSec = 0,
             queryCyclesPerSec = 0;
    // connections
    uint64_t maxConnections = 0, maxConnectionsPerUser = 0, maxSessionsPerUser = 0, maxOpenTxns = 0, maxTxnTimeMs = 0;
    // views
    uint64_t maxViewMemoryBytes = 0;
    // durability
    uint64_t minReplicationFactor = 0, maxReplicationFactor = 0, lowestAcks = 0, lowestMinInsync = 0;
    // features (bit per feature: Compaction, Txns, Ql, Views, Sql, Redis, AnonymousRead, AuthScram)
    uint32_t featureBits = 0;
    uint32_t reserved0 = 0;   // placement / password policy / encryption key: zero in Phase 1
    uint64_t reserved1 = 0, reserved2 = 0, reserved3 = 0;

    bool operator==(const NamespaceLimits&) const = default;
};

inline constexpr std::size_t kNamespaceLimitsBytes      = 28 * 8 + 4 + 4 + 3 * 8;   // 256
inline constexpr std::size_t kNamespaceLimitFieldCount  = 29;   // 28 u64 limits + featureBits

inline constexpr uint32_t kNsFeatureCompaction    = 1u << 0;
inline constexpr uint32_t kNsFeatureTxns          = 1u << 1;
inline constexpr uint32_t kNsFeatureQl            = 1u << 2;
inline constexpr uint32_t kNsFeatureViews         = 1u << 3;
inline constexpr uint32_t kNsFeatureSql           = 1u << 4;
inline constexpr uint32_t kNsFeatureRedis         = 1u << 5;
inline constexpr uint32_t kNsFeatureAnonymousRead = 1u << 6;
inline constexpr uint32_t kNsFeatureAuthScram     = 1u << 7;

static_assert(sizeof(NamespaceLimits) == kNamespaceLimitsBytes, "NamespaceLimits must be exactly 256 bytes, no padding");
static_assert(offsetof(NamespaceLimits, maxTopics) == 0);
static_assert(offsetof(NamespaceLimits, featureBits) == 28 * 8);
static_assert(offsetof(NamespaceLimits, reserved1) == 28 * 8 + 8);

namespace detail {
// The record is 28 u64 slots, then u32 featureBits, u32 reserved0, then 3 u64 slots; slot k of width w lives at the
// offset the declaration order gives it, so the wire order cannot drift from the struct.
inline constexpr std::size_t kLimitU64Head = 28;
inline void putLimitU64(uint8_t* out, uint64_t v) noexcept {
    for (int b = 0; b < 8; ++b) out[b] = static_cast<uint8_t>(v >> (8 * b));
}
inline void putLimitU32(uint8_t* out, uint32_t v) noexcept {
    for (int b = 0; b < 4; ++b) out[b] = static_cast<uint8_t>(v >> (8 * b));
}
inline uint64_t getLimitU64(const uint8_t* in) noexcept {
    uint64_t v = 0;
    for (int b = 0; b < 8; ++b) v |= static_cast<uint64_t>(in[b]) << (8 * b);
    return v;
}
inline uint32_t getLimitU32(const uint8_t* in) noexcept {
    uint32_t v = 0;
    for (int b = 0; b < 4; ++b) v |= static_cast<uint32_t>(in[b]) << (8 * b);
    return v;
}
} // namespace detail

/// Writes exactly kNamespaceLimitsBytes little-endian bytes at `out`.
inline void encodeLimits(const NamespaceLimits& l, uint8_t* out) noexcept {
    unsigned char raw[kNamespaceLimitsBytes];
    std::memcpy(raw, &l, kNamespaceLimitsBytes);   // slots in declaration order
    std::size_t o = 0;
    for (std::size_t i = 0; i < detail::kLimitU64Head; ++i, o += 8) {
        uint64_t v; std::memcpy(&v, raw + o, 8); detail::putLimitU64(out + o, v);
    }
    for (int i = 0; i < 2; ++i, o += 4) {
        uint32_t v; std::memcpy(&v, raw + o, 4); detail::putLimitU32(out + o, v);
    }
    for (int i = 0; i < 3; ++i, o += 8) {
        uint64_t v; std::memcpy(&v, raw + o, 8); detail::putLimitU64(out + o, v);
    }
}

/// Reads kNamespaceLimitsBytes bytes; false (nothing read past `len`) on a short buffer.
[[nodiscard]] inline bool decodeLimits(const uint8_t* in, std::size_t len, NamespaceLimits& out) noexcept {
    if (in == nullptr || len < kNamespaceLimitsBytes) return false;
    unsigned char raw[kNamespaceLimitsBytes];
    std::size_t o = 0;
    for (std::size_t i = 0; i < detail::kLimitU64Head; ++i, o += 8) {
        const uint64_t v = detail::getLimitU64(in + o); std::memcpy(raw + o, &v, 8);
    }
    for (int i = 0; i < 2; ++i, o += 4) {
        const uint32_t v = detail::getLimitU32(in + o); std::memcpy(raw + o, &v, 4);
    }
    for (int i = 0; i < 3; ++i, o += 8) {
        const uint64_t v = detail::getLimitU64(in + o); std::memcpy(raw + o, &v, 8);
    }
    std::memcpy(&out, raw, kNamespaceLimitsBytes);
    return true;
}

} // namespace rune::protocol
