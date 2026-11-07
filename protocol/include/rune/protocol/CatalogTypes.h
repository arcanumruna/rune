#pragma once
// rune/protocol/CatalogTypes.h — catalog history: the ONE definition of a topic's
// creation config (F32) and of a DDL request id (F14), shared by rune-codec's
// rune_catalog, the wire (client/SchemaMessages.h), the client and rune-ql.
// Header-only and std-only: rune-codec and rune-ql include it with no other
// protocol header.  Defaults = today's TopicSpec defaults; no other copy exists.
#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace rune::protocol {

/// Str = std::string (owned) or std::string_view (the wire's zero-copy decode).
template <class Str>
struct BasicTopicConfig {
    uint8_t cleanupPolicy             = 0;            // 0 Delete, 1 Compact, 2 CompactDelete
    int64_t retentionMs               = -1;
    int64_t retentionBytes            = -1;
    int64_t deleteRetentionMs         = 86'400'000;
    int32_t minCleanableDirtyRatioPpm = 500'000;      // 0.5
    int64_t segmentRollBytes          = 64LL * 1024 * 1024;
    int64_t minCompactionLagMs        = 0;
    int64_t maxCompactionLagMs        = -1;
    int64_t retentionCheckMs          = 300'000;
    int64_t fileDeleteDelayMs         = 60'000;
    uint8_t durability                = 0;            // 0 Majority, 1 AllInSync
    uint8_t durabilityTier            = 1;            // 0 Leader, 1 Majority, 2 Strict
    Str     coPartitionGroup{};
    Str     partitionKey{};
    bool operator==(const BasicTopicConfig&) const = default;
};
using TopicConfig     = BasicTopicConfig<std::string>;
using TopicConfigView = BasicTopicConfig<std::string_view>;

/// The one field-for-field copy between the two instantiations.
template <class To, class From>
[[nodiscard]] To convertTopicConfig(const From& f) {
    To t;
    t.cleanupPolicy = f.cleanupPolicy; t.retentionMs = f.retentionMs; t.retentionBytes = f.retentionBytes;
    t.deleteRetentionMs = f.deleteRetentionMs; t.minCleanableDirtyRatioPpm = f.minCleanableDirtyRatioPpm;
    t.segmentRollBytes = f.segmentRollBytes; t.minCompactionLagMs = f.minCompactionLagMs;
    t.maxCompactionLagMs = f.maxCompactionLagMs; t.retentionCheckMs = f.retentionCheckMs;
    t.fileDeleteDelayMs = f.fileDeleteDelayMs; t.durability = f.durability; t.durabilityTier = f.durabilityTier;
    t.coPartitionGroup = decltype(t.coPartitionGroup)(f.coPartitionGroup);
    t.partitionKey     = decltype(t.partitionKey)(f.partitionKey);
    return t;
}

/// AlterTopicConfig keys (ruling 5) — the ONE definition: the wire
/// (client/SchemaMessages.h AlterTopicConfig) and rune-codec's catalog events
/// (catalog::ConfigKey aliases it).  Append-only; values never move.
enum class ConfigKey : uint8_t {
    RetentionMs = 1, RetentionBytes = 2, DeleteRetentionMs = 3,
    MinCleanableDirtyRatioPpm = 4, MinCompactionLagMs = 5, MaxCompactionLagMs = 6,
    // Lease topics (catalog formatVer 4): alter the topic's LeaseSpec.  Valid only on a lease topic.  There are no keys for
    // lease.max_keys and lease.expiry_batch: they are fixed at creation.
    LeaseTtlMs                = 7,    ///< lease.ttl, ms (u64): the default TTL of a grant
    LeaseMinTtlMs             = 8,    ///< lease.min_ttl, ms (u64): the floor a grant's TTL is raised to
    LeaseMaxTtlMs             = 9,    ///< lease.max_ttl, ms (u64): the ceiling a grant's TTL is refused above
    LeaseCheckpoint           = 10,   ///< lease.checkpoint: 0 or 1
    LeaseCheckpointIntervalMs = 11,   ///< lease.checkpoint_interval, ms (u32)
    LeaseCheckpointRate       = 12,   ///< lease.checkpoint_rate (u32): leases checkpointed per second
};
inline constexpr ConfigKey kFirstConfigKey = ConfigKey::RetentionMs;
inline constexpr ConfigKey kLastConfigKey  = ConfigKey::LeaseCheckpointRate;
/// The last key catalog formatVer 1..3 accepts; the lease.* keys (7..12) need formatVer 4.
inline constexpr ConfigKey kLastConfigKeyV3 = ConfigKey::MaxCompactionLagMs;

/// A topic's key features (checked writes): the key index, autoincrement and the
/// key -> partition function.  One definition for the wire (DefineTopic,
/// TopicSchemaResponse), rune-codec's catalog events and the client.
enum class Partitioner : uint8_t { Fnv1a = 0, Modulo = 1 };
inline constexpr uint64_t kAutoIncrementMaxDefault     = (uint64_t{1} << 53) - 1;
inline constexpr uint32_t kAutoIncrementReserveDefault = 4096;
inline constexpr uint32_t kAutoIncrementReserveMax     = 1'048'576;
struct TopicKeyFeatures {
    bool        keyIndex             = false;
    bool        autoIncrement        = false;
    Partitioner partitioner          = Partitioner::Fnv1a;
    uint64_t    autoIncrementMax     = kAutoIncrementMaxDefault;
    uint32_t    autoIncrementReserve = kAutoIncrementReserveDefault;
    bool operator==(const TopicKeyFeatures&) const = default;
};

/// Wire block: [flags u8: bit0 keyIndex, bit1 autoIncrement][partitioner u8]
/// [autoIncrementMax u64 LE][autoIncrementReserve u32 LE].
inline constexpr std::size_t kTopicKeyFeaturesWireLen = 14;

inline void putTopicKeyFeatures(uint8_t* out, const TopicKeyFeatures& f) noexcept {
    out[0] = static_cast<uint8_t>((f.keyIndex ? 1u : 0u) | (f.autoIncrement ? 2u : 0u));
    out[1] = static_cast<uint8_t>(f.partitioner);
    for (int i = 0; i < 8; ++i) out[2 + i] = static_cast<uint8_t>(f.autoIncrementMax >> (8 * i));
    for (int i = 0; i < 4; ++i) out[10 + i] = static_cast<uint8_t>(f.autoIncrementReserve >> (8 * i));
}

/// False on a short buffer, an unknown flag bit or a partitioner above Modulo.
[[nodiscard]] inline bool getTopicKeyFeatures(const uint8_t* in, std::size_t avail, TopicKeyFeatures& out) noexcept {
    if (avail < kTopicKeyFeaturesWireLen || (in[0] & 0xFC) != 0 || in[1] > static_cast<uint8_t>(Partitioner::Modulo)) return false;
    out.keyIndex      = (in[0] & 1u) != 0;
    out.autoIncrement = (in[0] & 2u) != 0;
    out.partitioner   = static_cast<Partitioner>(in[1]);
    out.autoIncrementMax = 0;
    for (int i = 0; i < 8; ++i) out.autoIncrementMax |= uint64_t{in[2 + i]} << (8 * i);
    out.autoIncrementReserve = 0;
    for (int i = 0; i < 4; ++i) out.autoIncrementReserve |= uint32_t{in[10 + i]} << (8 * i);
    return true;
}

/// A DDL's logical id (F14): hi = the issuing client instance's random 64-bit id
/// (never 0), lo = that instance's counter.  {0, 0} = none (a pre-v8 client).
/// Stable across every resend of the same DDL.
struct RequestId {
    uint64_t hi = 0;
    uint64_t lo = 0;
    [[nodiscard]] constexpr bool none() const noexcept { return hi == 0 && lo == 0; }
    auto operator<=>(const RequestId&) const = default;
};
struct RequestIdHash {
    std::size_t operator()(const RequestId& r) const noexcept {
        return std::hash<uint64_t>{}(r.hi * 0x9E3779B97F4A7C15ull ^ r.lo);
    }
};

} // namespace rune::protocol
