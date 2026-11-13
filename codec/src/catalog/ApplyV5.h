#pragma once
// src/catalog/ApplyV5.h — the formatVer-5 apply rules (schema retire, #419): the formatVer-4 rules plus the
// RetireSchemaVersions kind, which raises a typed topic's retire floor.  Like its predecessors, `run` never mutates the
// state.  The floor lives in SchemaState::retiredBelow; it is hashed into the object fingerprint (and so the state
// digest) only when it is not 1, so a state with every floor at 1 keeps the digests every earlier log pinned.
#include "ApplyV4.h"

#include <cstdint>
#include <expected>

namespace rune::catalog {

struct ApplyV5 {
    /// RetireSchemaVersions: the gates every object kind passes, then the retire rule; every other kind runs ApplyV4::run.
    static std::expected<StagedV2, ApplyError> run(const CatalogState& s, const CatalogEvent& e, int64_t offset);
    static uint64_t fingerprintOfStaged(const CatalogEvent& e, const StagedV2& st) noexcept { return ApplyV4::fingerprintOfStaged(e, st); }
    static Applied commit(CatalogState& s, const CatalogEvent& e, int64_t offset, uint64_t fingerprint, StagedV2&& st) {
        return ApplyV4::commit(s, e, offset, fingerprint, std::move(st));
    }

    /// The retire floor of the staged topic's schema (1 for anything else).
    [[nodiscard]] static uint16_t retiredBelowOf(const StagedV2& st) noexcept;
    /// `fingerprint` for a floor of 1; otherwise it with the floor hashed after it.
    [[nodiscard]] static uint64_t retireFingerprint(uint64_t fingerprint, uint16_t retiredBelow) noexcept;
};

} // namespace rune::catalog
