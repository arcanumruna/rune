#pragma once
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Validator.h>

namespace rune::node {

inline constexpr int32_t  kSchemaTopicId        = -4;
inline constexpr uint32_t RUNTIME_TOPIC_ID_BASE  = 0x40000000u;

/// Wire record stored on the __topic_schemas internal topic.
/// serialize() LE layout:
///   topicId:i32, version:u16, partitionCount:i32, validate:u8,
///   keyFieldLen:u16 + keyField bytes,
///   descriptorLen:u32 + descriptorBlob bytes.
struct TopicSchema {
    int32_t               topicId        = -1;
    uint16_t              version        = 1;
    int32_t               partitionCount = 1;
    std::string           keyField;
    bool                  validate       = false;
    std::vector<uint8_t>  descriptorBlob;

    [[nodiscard]] std::vector<uint8_t> serialize() const;
    [[nodiscard]] static std::optional<TopicSchema> deserialize(std::span<const uint8_t> buf);
};

/// In-memory registry entry built from a TopicSchema.
struct RegistryEntry {
    int32_t                          topicId        = -1;
    rune::codec::Descriptor          descriptor;
    std::vector<uint8_t>             descriptorBlob;   // original blob (for GetTopicSchema)
    uint64_t                         fingerprint    = 0;
    std::optional<rune::codec::Validator> validator; // present iff validate=true
    std::string                      keyField;
    uint16_t                         version        = 1;
    int32_t                          partitionCount = 1;
};

/// Single-threaded, AdminWorker-owned schema registry.
/// No locks, no atomics — caller owns the thread.
class SchemaRegistry {
public:
    /// Apply (leader-commit or replica-apply) one schema record.
    void applyRecord(const TopicSchema& ts);

    /// Look up by numeric topic id. Returns nullptr if absent.
    [[nodiscard]] const RegistryEntry* byId(int32_t topicId) const;

    /// Look up by schema name (descriptor.name). Returns nullptr if absent.
    [[nodiscard]] const RegistryEntry* byName(std::string_view name) const;

    /// Allocate the next runtime topic id: returns hw, then hw++.
    [[nodiscard]] int32_t allocateRuntimeId();

    /// Startup replay: clear and re-apply a span of records.
    void rebuildFromRecords(std::span<const TopicSchema> records);

    [[nodiscard]] uint32_t runtimeIdHighWater() const noexcept { return runtimeIdHighWater_; }

private:
    std::unordered_map<int32_t, RegistryEntry> byId_;
    std::unordered_map<std::string, int32_t>   nameToId_;
    uint32_t runtimeIdHighWater_ = RUNTIME_TOPIC_ID_BASE;
};

} // namespace rune::node
