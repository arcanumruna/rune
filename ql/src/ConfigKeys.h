#pragma once
// Internal (not installed): the ONE table of topic settings shared by the parser
// (which validates them) and the backends (which apply them).  Alterable settings
// carry the rune::protocol::ConfigKey wire value; creation-only settings carry 0.
#include <array>
#include <cstdint>
#include <string_view>
#include <rune/protocol/CatalogTypes.h>

namespace rune::ql::detail {

enum class Value : uint8_t { Int, Ratio, Cleanup, Durability, Tier, String };
enum class Field : uint8_t {
    RetentionMs, RetentionBytes, DeleteRetentionMs, MinCleanableRatio, MinCompactionLagMs, MaxCompactionLagMs,
    CleanupPolicy, SegmentBytes, RetentionCheckMs, FileDeleteDelayMs, Durability, Tier, CoPartitionGroup, PartitionKey,
};
struct SettingSpec {
    std::string_view name;
    Value            value;
    Field            field;
    uint8_t          alterKey;   // rune::protocol::ConfigKey, 0 = creation-only
};
using protocol::ConfigKey;
constexpr uint8_t ck(ConfigKey k) { return static_cast<uint8_t>(k); }

inline constexpr std::array<SettingSpec, 14> kSettings{{
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
}};

[[nodiscard]] inline const SettingSpec* findSetting(std::string_view lowerName) noexcept {
    for (const auto& s : kSettings) if (s.name == lowerName) return &s;
    return nullptr;
}
[[nodiscard]] inline const SettingSpec* findSettingByField(Field f) noexcept {
    for (const auto& s : kSettings) if (s.field == f) return &s;
    return nullptr;
}

} // namespace rune::ql::detail
