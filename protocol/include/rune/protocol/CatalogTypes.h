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
