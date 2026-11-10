#pragma once
// src/catalog/ApplyV4.h — the formatVer-4 apply rules (lease topics): the formatVer-3 rules plus the lease trailer of a
// CreateTopic (which creates the lease view beside the topic in the same commit) and the lease.* AlterTopicConfig keys.
// The state-driven lease rules (the lease view cannot be dropped or altered on its own, DropTopic takes it along, a
// lease topic keeps every live lease, the system columns stay) run for EVERY formatVer: a lease view exists only after
// a formatVer-4 event, so they are no-ops on any older log.  Like its predecessors, `run` never mutates the state.
#include "ApplyV3.h"

#include <rune/protocol/LeaseSpec.h>

#include <cstdint>
#include <expected>
#include <optional>
#include <utility>

namespace rune::catalog {

struct ApplyV4 {
    /// ApplyV3::run, then the lease rules: a CreateTopic's lease and the lease.* keys of an AlterTopicConfig.
    static std::expected<StagedV2, ApplyError> run(const CatalogState& s, const CatalogEvent& e, int64_t offset);
    static uint64_t fingerprintOfStaged(const CatalogEvent& e, const StagedV2& st) noexcept { return ApplyV3::fingerprintOfStaged(e, st); }
    /// ApplyV3::commit, then a lease CreateTopic installs the lease view and consumes one more object id and topic id.
    static Applied commit(CatalogState& s, const CatalogEvent& e, int64_t offset, uint64_t fingerprint, StagedV2&& st);

    // ── state-driven rules, every formatVer ─────────────────────────────────────────────────────────────────────────
    /// Before the versioned rules: DropView / AlterView naming a lease view; AlterSchema dropping or renaming a system
    /// column of a lease topic or adding a `lease_` field.  nullopt = fine.
    [[nodiscard]] static std::optional<ApplyError> guardBefore(const CatalogState& s, const CatalogEvent& e, int64_t offset);
    /// After the versioned rules: an AlterTopicConfig that would leave a lease topic with retention.
    [[nodiscard]] static std::optional<ApplyError> guardStaged(const CatalogEvent& e, const StagedV2& st, int64_t offset);
    /// NamespaceDeleted: the lease views leave the staged pending set (they go with their topic).
    static void excludeLeaseViews(const CatalogState& s, StagedV2& st);
    /// DropTopic of a lease topic (after the versioned commit): drops its lease view and tombstones it.
    static void commitLeaseViewDrop(CatalogState& s, const CatalogEvent& e, Applied& applied);

    // ── fingerprints ────────────────────────────────────────────────────────────────────────────────────────────────
    /// The lease of the staged topic (nullopt for anything else).
    [[nodiscard]] static std::optional<rune::protocol::LeaseSpec> leaseOf(const StagedV2& st) noexcept;
    /// `fingerprint` for a topic without a lease; otherwise it with the encoded LeaseSpec hashed after it.
    [[nodiscard]] static uint64_t leaseFingerprint(uint64_t fingerprint, const std::optional<rune::protocol::LeaseSpec>& lease) noexcept;
    /// What a lease view contributes to CatalogState::digest() instead of its plain fingerprint.
    [[nodiscard]] static uint64_t leaseViewFingerprint(uint64_t viewFingerprint) noexcept;
};

} // namespace rune::catalog
