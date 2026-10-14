#include "rune/node/SchemaRegistry.h"
#include <rune/codec/Evolve.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <algorithm>
#include <cstring>

namespace rune::node {

// ---------------------------------------------------------------------------
// SchemaRegistry
// ---------------------------------------------------------------------------
void SchemaRegistry::applyRecord(const TopicSchema& ts) {
    RegistryEntry e;
    e.topicId        = ts.topicId;
    e.descriptorBlob = ts.descriptorBlob;

    // `ts.descriptorBlob` carries the cumulative EVOLVING descriptor (every
    // field ever declared for this topic, live or retired). The stored
    // registry entry must reflect the live field set *as of ts.version*
    // (matching offsets/blockLength a record written at that version was
    // actually encoded with) — a fresh v1 topic's evolving descriptor has
    // no retired/future fields, so materializeAsOf is an identity there.
    const rune::codec::Descriptor evolving = rune::codec::deserialize(ts.descriptorBlob);
    e.descriptor     = rune::codec::materializeAsOf(evolving, ts.version);
    e.fingerprint    = rune::codec::fingerprintAsOf(evolving, ts.version);
    e.validator      = ts.validate
                         ? std::optional<rune::codec::Validator>(std::in_place, e.descriptor)
                         : std::nullopt;
    e.keyField       = ts.keyField;
    e.version        = ts.version;
    e.partitionCount = ts.partitionCount;
    e.nextTag        = ts.nextTag;
    e.compatMode     = ts.compatMode;
    e.topicName      = ts.topicName;
    e.persistence    = ts.persistence;

    // Index by the TOPIC name — every byName() caller (GetTopicSchema,
    // DefineTopic's TopicExists check, AlterTopic) passes the name the client
    // defined the topic under, which is independent of the descriptor's name
    // (Phase 2b C regression: "typed_route" with the "characters" descriptor
    // answered UnknownTopic).  The descriptor name is only the FALLBACK for a
    // record applied with an empty topicName: a pre-name-trailer durable record
    // (where it is the only name the topic can be found under), or any in-memory
    // caller that leaves it unset.  It is NOT legacy-only by construction — every
    // in-tree caller must set topicName (DefineTopic commit + replay, ALTER's
    // carry-forward, CREATE VIEW's commitCreateView_ and view recovery all do),
    // or the entry lands under its descriptor name and can shadow a real topic.
    const std::string& indexName = e.topicName.empty() ? e.descriptor.name : e.topicName;
    nameToId_[indexName] = ts.topicId;

    // Keep ALL versions: append (never overwrite) by version, 1-based index.
    // byId_'s per-topic container is a deque (not a vector) specifically so
    // growing it here never invalidates a `const RegistryEntry*` a caller
    // already holds from a prior byId/latest/byIdVersion/byFingerprint call.
    auto& versions = byId_[ts.topicId];
    const size_t idx = (ts.version > 0) ? static_cast<size_t>(ts.version - 1) : size_t{0};
    if (idx >= versions.size()) versions.resize(idx + 1);
    versions[idx] = std::move(e);
}

const RegistryEntry* SchemaRegistry::byId(int32_t topicId) const {
    return latest(topicId);
}

const RegistryEntry* SchemaRegistry::byName(std::string_view name) const {
    const auto it = nameToId_.find(std::string(name));
    if (it == nameToId_.end()) return nullptr;
    return latest(it->second);
}

const RegistryEntry* SchemaRegistry::byIdVersion(int32_t topicId, uint16_t version) const {
    const auto it = byId_.find(topicId);
    if (it == byId_.end() || version == 0 || version > it->second.size()) return nullptr;
    const RegistryEntry& e = it->second[version - 1];
    return (e.topicId == topicId) ? &e : nullptr;  // guards an unfilled gap slot
}

const RegistryEntry* SchemaRegistry::latest(int32_t topicId) const {
    const auto it = byId_.find(topicId);
    if (it == byId_.end() || it->second.empty()) return nullptr;
    const RegistryEntry& e = it->second.back();
    return (e.topicId == topicId) ? &e : nullptr;  // guards an unfilled gap slot
}

const RegistryEntry* SchemaRegistry::byFingerprint(int32_t topicId, uint64_t fp) const {
    const auto it = byId_.find(topicId);
    if (it == byId_.end()) return nullptr;
    for (const auto& e : it->second) {
        if (e.topicId == topicId && e.fingerprint == fp) return &e;
    }
    return nullptr;
}

uint16_t SchemaRegistry::nextTagFor(int32_t topicId) const {
    const RegistryEntry* e = latest(topicId);
    return e ? e->nextTag : 0;
}

bool SchemaRegistry::erase(int32_t topicId) {
    const bool had = byId_.erase(topicId) > 0;
    for (auto it = nameToId_.begin(); it != nameToId_.end();) {
        if (it->second == topicId) it = nameToId_.erase(it);
        else ++it;
    }
    return had;
}

} // namespace rune::node
