#pragma once
// src/catalog/ApplyV3.h — the formatVer-3 apply rules: the formatVer-2 rules plus the topic key features
// (rune::protocol::TopicKeyFeatures) that a CreateTopic / CreateSchema carries.  Like its predecessors, `run` never
// mutates the state.
#include "ApplyV2.h"

#include <rune/protocol/CatalogTypes.h>

#include <cstdint>
#include <expected>
#include <utility>

namespace rune::catalog {

struct ApplyV3 {
    /// ApplyV2::run, then the staged topic takes the event's keyFeatures; `keyIndex` needs a key field and a Disk topic.
    static std::expected<StagedV2, ApplyError> run(const CatalogState& s, const CatalogEvent& e, int64_t offset);
    static uint64_t fingerprintOfStaged(const CatalogEvent& e, const StagedV2& st) noexcept { return ApplyV2::fingerprintOfStaged(e, st); }
    static Applied commit(CatalogState& s, const CatalogEvent& e, int64_t offset, uint64_t fingerprint, StagedV2&& st) {
        return ApplyV2::commit(s, e, offset, fingerprint, std::move(st));
    }

    /// A staged topic that already had a schema keeps that schema's keyFeatures (ApplyV1 rebuilds SchemaState without
    /// them).  Runs for every formatVer; a no-op while the features are default.
    static void carryKeyFeatures(const CatalogState& s, StagedV2& st) noexcept;
    /// The keyFeatures of the staged topic's schema (default for anything else).
    [[nodiscard]] static rune::protocol::TopicKeyFeatures keyFeaturesOf(const StagedV2& st) noexcept;
    /// `fingerprint` for default features; otherwise it with the features hashed after it.
    [[nodiscard]] static uint64_t keyFeaturesFingerprint(uint64_t fingerprint, const rune::protocol::TopicKeyFeatures& features) noexcept;
};

} // namespace rune::catalog
