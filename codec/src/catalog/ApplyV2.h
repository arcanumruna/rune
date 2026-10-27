#pragma once
// src/catalog/ApplyV2.h — the formatVer-2 apply rules (plan D5).  Object kinds (1..9) run the shared object rules
// (ApplyV1.cpp) per namespace behind a gate: the event's namespace must exist and be ACTIVE.  The namespace kinds
// (32..35) are defined here.  Like ApplyV1, `run` never mutates the state and `commit` installs what it produced.
#include "ApplyV1.h"

#include <rune/catalog/CatalogState.h>

#include <cstdint>
#include <expected>
#include <string>
#include <variant>

namespace rune::catalog {

struct StagedNsCreate { NamespaceRecord record; };                 // NamespaceCreated
struct StagedNsUpdate { NamespaceRecord record; };                 // NamespaceLimitsSet, NamespaceStateChanged
struct StagedNsDelete { uint32_t nsId; std::string name; uint32_t finalVersion; };   // NamespaceDeleted
using StagedV2 = std::variant<Staged, StagedNsCreate, StagedNsUpdate, StagedNsDelete>;

struct ApplyV2 {
    static std::expected<StagedV2, ApplyError> run(const CatalogState& s, const CatalogEvent& e, int64_t offset);
    /// The fingerprint `e` must carry: objectFingerprintV2 for an object kind, the record's / tombstone's for a
    /// namespace kind.
    static uint64_t fingerprintOfStaged(const CatalogEvent& e, const StagedV2& st) noexcept;
    static Applied commit(CatalogState& s, const CatalogEvent& e, int64_t offset, uint64_t fingerprint, StagedV2&& st);
};

} // namespace rune::catalog
