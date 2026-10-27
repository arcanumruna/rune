#pragma once
#include <cstdint>
#include <deque>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Validator.h>
#include <rune/gateway/NsNameKey.h>
#include <rune/node/PersistenceMode.h>

namespace rune::node {

/// One topic schema version as the SchemaRegistry applies it.  Catalog history
/// Task 16: this is only the CatalogApplier's projection input (a TopicObject's
/// SchemaState); the retired `__topic_schemas` wire record and its
/// serialize / deserialize are gone — `__catalog` is the catalog.
struct TopicSchema {
    int32_t               topicId        = -1;
    uint16_t              version        = 1;
    int32_t               partitionCount = 1;
    std::string           keyField;
    bool                  validate       = false;
    std::vector<uint8_t>  descriptorBlob;
    uint16_t              nextTag        = 0;    // next durable tag to allocate on ADD
    rune::codec::CompatMode compatMode   = rune::codec::CompatMode::Backward;
    // The TOPIC name this schema belongs to (Client::defineTopic takes it
    // independently of the descriptor's name).
    std::string           topicName;
    // Phase 2b E: the topic's persistence mode.
    PersistenceMode       persistence    = PersistenceMode::Disk;
    // Namespaces Task 5: the namespace that owns the topic (names are per namespace).
    uint32_t              nsId           = 0;
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
    rune::codec::CompatMode          compatMode     = rune::codec::CompatMode::Backward;
    PersistenceMode                  persistence    = PersistenceMode::Disk;   // Phase 2b E
    std::string                      topicName;        // durable topic name (may be empty: legacy record)
    uint32_t                         nsId           = 0;
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

    /// Look up the LATEST version by TOPIC name (the name DefineTopic / CREATE VIEW
    /// was given). A record applied with an EMPTY topicName — a pre-name-trailer
    /// durable record, or any caller that leaves it unset — is indexed by its
    /// descriptor.name instead, so every caller must set topicName. Returns
    /// nullptr if absent.
    [[nodiscard]] const RegistryEntry* byName(uint32_t nsId, std::string_view name) const;
    /// The `default` namespace (nsId 0) form.
    [[nodiscard]] const RegistryEntry* byName(std::string_view name) const { return byName(0, name); }

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

    /// Catalog history Task 7: forget every version of `topicId` (a DropTopic /
    /// DropView / AlterView-retired sink applied).  Its name stops resolving (only
    /// when it still names THIS id — a reused name already re-pointed is kept).
    /// Invalidates every `const RegistryEntry*` previously returned for `topicId`
    /// (only); an unknown id is a no-op.  Returns whether anything was erased.
    bool erase(int32_t topicId);

private:
    // topicId -> all applied versions, indexed by (version - 1); versions
    // are applied in order by the CatalogApplier so this never
    // has gaps in practice. Single-writer (admin-thread-owned) — no atomics.
    //
    // std::deque, NOT std::vector: callers (e.g. Task 11's ALTER flow) hold
    // a `const RegistryEntry*` from byId/latest/byIdVersion/byFingerprint
    // across a subsequent applyRecord() of a new version. A vector's
    // push_back/resize may reallocate and invalidate every existing element
    // pointer; a deque's insertion/growth at the back invalidates iterators
    // but never references/pointers to already-existing elements, so those
    // pointers stay valid.
    std::unordered_map<int32_t, std::deque<RegistryEntry>> byId_;
    // (nsId, topic name) -> topicId; transparent hash, so a lookup builds no std::string.
    std::unordered_map<rune::gateway::NsName, int32_t, rune::gateway::NsNameHash, rune::gateway::NsNameEq> nameToId_;
};

} // namespace rune::node
