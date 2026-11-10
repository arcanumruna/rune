#pragma once
// Internal (not installed): the ONE table of topic settings shared by the parser
// (which validates them) and the backends (which apply them).  Alterable settings
// carry the rune::protocol::ConfigKey wire value; creation-only settings carry 0.
#include <array>
#include <cstdint>
#include <limits>
#include <optional>
#include <string_view>
#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/LeaseSpec.h>

namespace rune::ql::detail {

enum class Value : uint8_t { Int, Ratio, Cleanup, Durability, Tier, String, OnOff };
enum class Field : uint8_t {
    RetentionMs, RetentionBytes, DeleteRetentionMs, MinCleanableRatio, MinCompactionLagMs, MaxCompactionLagMs,
    CleanupPolicy, SegmentBytes, RetentionCheckMs, FileDeleteDelayMs, Durability, Tier, CoPartitionGroup, PartitionKey,
    KeyIndex, AutoIncrementReserve, AutoIncrementMax,
    LeaseTtl, LeaseMinTtl, LeaseMaxTtl, LeaseMaxKeys, LeaseExpiryBatch, LeaseCheckpoint, LeaseCheckpointInterval,
    LeaseCheckpointRate,
};
struct SettingSpec {
    std::string_view name;
    Value            value;
    Field            field;
    uint8_t          alterKey;   // rune::protocol::ConfigKey, 0 = creation-only
};
using protocol::ConfigKey;
constexpr uint8_t ck(ConfigKey k) { return static_cast<uint8_t>(k); }

inline constexpr std::array<SettingSpec, 25> kSettings{{
    {"retention.ms",               Value::Int,        Field::RetentionMs,        ck(ConfigKey::RetentionMs)},
    {"retention.bytes",            Value::Int,        Field::RetentionBytes,     ck(ConfigKey::RetentionBytes)},
    {"delete.retention.ms",        Value::Int,        Field::DeleteRetentionMs,  ck(ConfigKey::DeleteRetentionMs)},
    {"min.cleanable.dirty.ratio",  Value::Ratio,      Field::MinCleanableRatio,  ck(ConfigKey::MinCleanableDirtyRatioPpm)},
    {"min.compaction.lag.ms",      Value::Int,        Field::MinCompactionLagMs, ck(ConfigKey::MinCompactionLagMs)},
    {"max.compaction.lag.ms",      Value::Int,        Field::MaxCompactionLagMs, ck(ConfigKey::MaxCompactionLagMs)},
    {"cleanup.policy",             Value::Cleanup,    Field::CleanupPolicy,      0},
    {"segment.bytes",              Value::Int,        Field::SegmentBytes,       0},
    {"retention.check.ms",         Value::Int,        Field::RetentionCheckMs,   0},
    {"file.delete.delay.ms",       Value::Int,        Field::FileDeleteDelayMs,  0},
    {"durability",                 Value::Durability, Field::Durability,         0},
    {"tier",                       Value::Tier,       Field::Tier,               0},
    {"copartition.group",          Value::String,     Field::CoPartitionGroup,   0},
    {"partition.key",              Value::String,     Field::PartitionKey,       0},
    {"key.index",                  Value::OnOff,      Field::KeyIndex,           0},
    {"autoincrement.reserve",      Value::Int,        Field::AutoIncrementReserve, 0},
    {"autoincrement.max",          Value::Int,        Field::AutoIncrementMax,   0},
    // Lease topics (names as the catalog and validateLeaseSpec spell them); the ranges are applyLeaseSetting's.
    {"lease.ttl",                  Value::Int,        Field::LeaseTtl,           ck(ConfigKey::LeaseTtlMs)},
    {"lease.min_ttl",              Value::Int,        Field::LeaseMinTtl,        ck(ConfigKey::LeaseMinTtlMs)},
    {"lease.max_ttl",              Value::Int,        Field::LeaseMaxTtl,        ck(ConfigKey::LeaseMaxTtlMs)},
    {"lease.max_keys",             Value::Int,        Field::LeaseMaxKeys,       0},
    {"lease.expiry_batch",         Value::Int,        Field::LeaseExpiryBatch,   0},
    {"lease.checkpoint",           Value::OnOff,      Field::LeaseCheckpoint,    ck(ConfigKey::LeaseCheckpoint)},
    {"lease.checkpoint_interval",  Value::Int,        Field::LeaseCheckpointInterval, ck(ConfigKey::LeaseCheckpointIntervalMs)},
    {"lease.checkpoint_rate",      Value::Int,        Field::LeaseCheckpointRate, ck(ConfigKey::LeaseCheckpointRate)},
}};

[[nodiscard]] inline const SettingSpec* findSetting(std::string_view lowerName) noexcept {
    for (const auto& s : kSettings) if (s.name == lowerName) return &s;
    return nullptr;
}
[[nodiscard]] inline const SettingSpec* findSettingByField(Field f) noexcept {
    for (const auto& s : kSettings) if (s.field == f) return &s;
    return nullptr;
}

[[nodiscard]] inline bool isLeaseField(Field f) noexcept {
    return f >= Field::LeaseTtl && f <= Field::LeaseCheckpointRate;
}

/// The ONE apply of a `lease.*` setting, used by the parser (to validate) and the backend (to plan).  A negative value or
/// one above the target member's maximum returns that key's range text; otherwise it is stored and nullopt returned.
/// The cross rules (ttl vs min/max) are validateLeaseSpec's.
[[nodiscard]] inline std::optional<std::string_view> applyLeaseSetting(protocol::LeaseSpec& s, Field f, int64_t v) noexcept {
    const auto fits = [v](uint64_t max) { return v >= 0 && static_cast<uint64_t>(v) <= max; };
    switch (f) {
        case Field::LeaseTtl:
            if (!fits(std::numeric_limits<uint64_t>::max())) return protocol::kLeaseTtlRangeText;
            s.ttlMs = static_cast<uint64_t>(v);
            return std::nullopt;
        case Field::LeaseMinTtl:
            if (!fits(std::numeric_limits<uint64_t>::max())) return protocol::kLeaseMinTtlRangeText;
            s.minTtlMs = static_cast<uint64_t>(v);
            return std::nullopt;
        case Field::LeaseMaxTtl:
            if (!fits(std::numeric_limits<uint64_t>::max())) return protocol::kLeaseMaxTtlRangeText;
            s.maxTtlMs = static_cast<uint64_t>(v);
            return std::nullopt;
        case Field::LeaseMaxKeys:
            if (!fits(std::numeric_limits<uint32_t>::max())) return protocol::kLeaseMaxKeysRangeText;
            s.maxKeys = static_cast<uint32_t>(v);
            return std::nullopt;
        case Field::LeaseExpiryBatch:
            if (!fits(std::numeric_limits<uint16_t>::max())) return protocol::kLeaseExpiryBatchRangeText;
            s.expiryBatch = static_cast<uint16_t>(v);
            return std::nullopt;
        case Field::LeaseCheckpoint:
            if (!fits(std::numeric_limits<uint8_t>::max())) return protocol::kLeaseCheckpointText;
            s.checkpoint = static_cast<uint8_t>(v);
            return std::nullopt;
        case Field::LeaseCheckpointInterval:
            if (!fits(std::numeric_limits<uint32_t>::max())) return protocol::kLeaseCheckpointIntervalRangeText;
            s.checkpointIntervalMs = static_cast<uint32_t>(v);
            return std::nullopt;
        case Field::LeaseCheckpointRate:
            if (!fits(std::numeric_limits<uint32_t>::max())) return protocol::kLeaseCheckpointRateRangeText;
            s.checkpointRate = static_cast<uint32_t>(v);
            return std::nullopt;
        default: return std::string_view("not a lease setting");
    }
}

} // namespace rune::ql::detail
