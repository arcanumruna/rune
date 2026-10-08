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
///   descriptorLen:u32 + descriptorBlob bytes,
///   nextTag:u16, compatMode:u8.
/// The trailing (nextTag, compatMode) pair is APPENDED after the original
/// layout (schema-evolution, Task 9) so legacy records written before these
/// fields existed still deserialize: when the trailing bytes are absent,
/// deserialize() defaults compatMode=0 (BACKWARD) and nextTag to the field
/// count of the embedded descriptor.
struct TopicSchema {
    int32_t               topicId        = -1;
    uint16_t              version        = 1;
    int32_t               partitionCount = 1;
    std::string           keyField;
    bool                  validate       = false;
    std::vector<uint8_t>  descriptorBlob;
    uint16_t              nextTag        = 0;    // next durable tag to allocate on ADD
    uint8_t               compatMode     = 0;    // 0=BACKWARD, 1=FORWARD

    [[nodiscard]] std::vector<uint8_t> serialize() const;
    [[nodiscard]] static std::optional<TopicSchema> deserialize(std::span<const uint8_t> buf);
};

/// In-memory registry entry built from a TopicSchema.
/// `descriptor` and `fingerprint` are the materialized-as-of-`version` view
/// (rune::codec::materializeAsOf / fingerprintAsOf) of the evolving
/// descriptor carried in `descriptorBlob` — i.e. exactly the live field set
/// (and matching offsets/blockLength) a record written against this schema
/// version would have been encoded with.
struct RegistryEntry {
    int32_t                          topicId        = -1;
    rune::codec::Descriptor          descriptor;
    std::vector<uint8_t>             descriptorBlob;   // original/evolving blob (for GetTopicSchema)
    uint64_t                         fingerprint    = 0;
    std::optional<rune::codec::Validator> validator; // present iff validate=true
    std::string                      keyField;
    uint16_t                         version        = 1;
    int32_t                          partitionCount = 1;
    uint16_t                         nextTag        = 0;
    uint8_t                          compatMode     = 0;
};

/// Single-threaded, AdminWorker-owned schema registry.
/// No locks, no atomics — caller owns the thread.
class SchemaRegistry {
public:
    /// Apply (leader-commit or replica-apply) one schema record.
    /// Precondition: ts.descriptorBlob is a valid, pre-validated descriptor blob.
    /// May throw std::runtime_error (propagated from rune::codec::deserialize) if
    /// the blob is malformed or truncated.
    void applyRecord(const TopicSchema& ts);

    /// Look up the LATEST version by numeric topic id. Returns nullptr if absent.
    [[nodiscard]] const RegistryEntry* byId(int32_t topicId) const;

    /// Look up the LATEST version by schema name (descriptor.name). Returns nullptr if absent.
    [[nodiscard]] const RegistryEntry* byName(std::string_view name) const;

    /// Look up one specific version of a topic's schema. Returns nullptr if
    /// that topic or version was never applied.
    [[nodiscard]] const RegistryEntry* byIdVersion(int32_t topicId, uint16_t version) const;

    /// Look up the latest (highest-version) schema for a topic. Returns
    /// nullptr if the topic has no applied schema.
    [[nodiscard]] const RegistryEntry* latest(int32_t topicId) const;

    /// Look up a version of a topic's schema by its (version-scoped)
    /// fingerprint — used to resolve an old producer's stamped fingerprint
    /// back to the exact historical version it was written against.
    /// Returns nullptr if no version of this topic matches.
    [[nodiscard]] const RegistryEntry* byFingerprint(int32_t topicId, uint64_t fp) const;

    /// Next durable tag to allocate for this topic's next ADD (from the
    /// latest applied version). Returns 0 if the topic has no applied schema.
    [[nodiscard]] uint16_t nextTagFor(int32_t topicId) const;

    /// Allocate the next runtime topic id: returns hw, then hw++.
    [[nodiscard]] int32_t allocateRuntimeId();

    /// Startup replay: clear and re-apply a span of records.
    void rebuildFromRecords(std::span<const TopicSchema> records);

    [[nodiscard]] uint32_t runtimeIdHighWater() const noexcept { return runtimeIdHighWater_; }

private:
    // topicId -> all applied versions, indexed by (version - 1); versions
    // are applied in order by AdminWorker/rebuildFromRecords so this never
    // has gaps in practice. Single-writer (admin-thread-owned) — no atomics.
    std::unordered_map<int32_t, std::vector<RegistryEntry>> byId_;
    std::unordered_map<std::string, int32_t>   nameToId_;
    uint32_t runtimeIdHighWater_ = RUNTIME_TOPIC_ID_BASE;
};

} // namespace rune::node
