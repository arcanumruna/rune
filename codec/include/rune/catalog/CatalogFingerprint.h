#pragma once
// rune/catalog/CatalogFingerprint.h — the fingerprint of a catalog object's resulting
// state (spec §4, §6; ruling 11).  FNV-1a-64, every variable-length field length-
// prefixed, and NEVER an id (objectId, topicId, viewTopicId, compiled plan): two
// clusters holding the same definition under different ids hash equal.  The field
// order below is frozen with formatVer 1 (golden logs pin the values, Task 3).
//
//   topic:     'T', name, version, partitions, replicationFactor, persistence,
//              every TopicConfig field in declaration order, hasSchema
//              [, keyField, validate, evolvingBlob, schemaVersion, nextTag, compat]
//   view:      'V', name, version, sinkPartitions, persistence, keyField,
//              source count + each source name, normalizedPlanBlob,
//              rune::codec::fingerprint(deserialize(outputDescriptorBlob)) (0 when it
//              does not deserialize — apply refuses such an event first)
//   tombstone: 'D', wasView, name, finalVersion
#include <rune/catalog/CatalogState.h>

#include <cstddef>
#include <cstdint>

namespace rune::catalog {

[[nodiscard]] uint64_t fingerprintOf(const TopicObject& t) noexcept;
[[nodiscard]] uint64_t fingerprintOf(const ViewObject& v) noexcept;
[[nodiscard]] uint64_t fingerprintOf(const Tombstone& d) noexcept;

namespace detail {
/// The FNV building blocks the fingerprints and CatalogState::digest() share.
/// Integers little-endian; `bytes` = u32 LE length, then each byte.
[[nodiscard]] uint64_t fnvU64(uint64_t h, uint64_t v) noexcept;
[[nodiscard]] uint64_t fnvBytes(uint64_t h, const void* data, std::size_t n) noexcept;
} // namespace detail

} // namespace rune::catalog
