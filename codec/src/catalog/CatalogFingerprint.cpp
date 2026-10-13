// src/catalog/CatalogFingerprint.cpp — see CatalogFingerprint.h for the frozen field order.
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>

#include <string_view>

namespace rune::catalog {

namespace cdd = rune::codec::detail;

namespace detail {
uint64_t fnvU64(uint64_t h, uint64_t v) noexcept {
    h = cdd::fnv1a_u32le(h, static_cast<uint32_t>(v & 0xFFFF'FFFFull));
    return cdd::fnv1a_u32le(h, static_cast<uint32_t>(v >> 32));
}
uint64_t fnvBytes(uint64_t h, const void* data, std::size_t n) noexcept {
    // Every catalog string / blob fits a u32 length (the event's widest prefix is u32).
    h = cdd::fnv1a_u32le(h, static_cast<uint32_t>(n));
    const auto* p = static_cast<const uint8_t*>(data);
    for (std::size_t i = 0; i < n; ++i) h = cdd::fnv1a_byte(h, p[i]);
    return h;
}
} // namespace detail

namespace {
uint64_t str(uint64_t h, std::string_view s) noexcept { return detail::fnvBytes(h, s.data(), s.size()); }
uint64_t blob(uint64_t h, const std::vector<uint8_t>& b) noexcept { return detail::fnvBytes(h, b.data(), b.size()); }
uint64_t i64(uint64_t h, int64_t v) noexcept { return detail::fnvU64(h, static_cast<uint64_t>(v)); }
uint64_t i32(uint64_t h, int32_t v) noexcept { return cdd::fnv1a_u32le(h, static_cast<uint32_t>(v)); }
uint64_t u8(uint64_t h, uint8_t v) noexcept { return cdd::fnv1a_byte(h, v); }

uint64_t config(uint64_t h, const TopicConfig& c) noexcept {   // declaration order (CatalogTypes.h)
    h = u8(h, c.cleanupPolicy);
    h = i64(h, c.retentionMs);
    h = i64(h, c.retentionBytes);
    h = i64(h, c.deleteRetentionMs);
    h = i32(h, c.minCleanableDirtyRatioPpm);
    h = i64(h, c.segmentRollBytes);
    h = i64(h, c.minCompactionLagMs);
    h = i64(h, c.maxCompactionLagMs);
    h = i64(h, c.retentionCheckMs);
    h = i64(h, c.fileDeleteDelayMs);
    h = u8(h, c.durability);
    h = u8(h, c.durabilityTier);
    h = str(h, c.coPartitionGroup);
    h = str(h, c.partitionKey);
    return h;
}

/// The view's output descriptor as a DEFINITION (equal however it was serialized):
/// rune::codec::fingerprint (name; per field, by tag: tag, type, since, until, key, name,
/// enums, fixedLen), then what that leaves out but the definition fixes — the descriptor
/// version and the field DECLARATION order (it fixes the block layout records use).
uint64_t outputDescriptorFingerprint(uint64_t h, const std::vector<uint8_t>& b) noexcept {
    try {
        const rune::codec::Descriptor d = rune::codec::deserialize(std::span<const uint8_t>(b.data(), b.size()));
        h = detail::fnvU64(h, rune::codec::fingerprint(d));
        h = cdd::fnv1a_u16le(h, d.version);
        h = cdd::fnv1a_u32le(h, static_cast<uint32_t>(d.fields.size()));
        for (const auto& f : d.fields) h = cdd::fnv1a_u16le(h, f.id);
        return h;
    } catch (...) {
        return detail::fnvU64(h, 0);   // apply refuses an undeserializable output descriptor before hashing
    }
}
} // namespace

uint64_t fingerprintOf(const TopicObject& t) noexcept {
    uint64_t h = cdd::kFnvSeed;
    h = u8(h, 'T');
    h = str(h, t.name);
    h = cdd::fnv1a_u32le(h, t.version);
    h = i32(h, t.partitions);
    h = cdd::fnv1a_u16le(h, static_cast<uint16_t>(t.replicationFactor));
    h = u8(h, static_cast<uint8_t>(t.persistence));
    h = config(h, t.config);
    h = u8(h, t.schema ? 1 : 0);
    if (t.schema) {
        const SchemaState& s = *t.schema;
        h = str(h, s.keyField);
        h = u8(h, s.validate ? 1 : 0);
        h = blob(h, s.evolvingBlob);
        h = cdd::fnv1a_u16le(h, s.schemaVersion);
        h = cdd::fnv1a_u16le(h, s.nextTag);
        h = u8(h, static_cast<uint8_t>(s.compat));
    }
    return h;
}

uint64_t fingerprintOf(const ViewObject& v) noexcept {
    uint64_t h = cdd::kFnvSeed;
    h = u8(h, 'V');
    h = str(h, v.name);
    h = cdd::fnv1a_u32le(h, v.version);
    h = i32(h, v.def.sinkPartitions);
    h = u8(h, static_cast<uint8_t>(v.def.persistence));
    h = str(h, v.def.keyField);
    h = cdd::fnv1a_u32le(h, static_cast<uint32_t>(v.def.sourceNames.size()));
    for (const std::string& s : v.def.sourceNames) h = str(h, s);
    h = blob(h, v.def.normalizedPlanBlob);
    h = outputDescriptorFingerprint(h, v.def.outputDescriptorBlob);
    return h;
}

uint64_t fingerprintOf(const Tombstone& d) noexcept {
    uint64_t h = cdd::kFnvSeed;
    h = u8(h, 'D');
    h = u8(h, d.wasView ? 1 : 0);
    h = str(h, d.name);
    h = cdd::fnv1a_u32le(h, d.finalVersion);
    return h;
}

} // namespace rune::catalog
