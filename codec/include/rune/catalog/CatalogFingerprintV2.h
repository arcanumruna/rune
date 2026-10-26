#pragma once
// rune/catalog/CatalogFingerprintV2.h — the formatVer-2 fingerprints (plan D4).  CatalogFingerprint.h stays
// FROZEN with formatVer 1; the namespace-aware functions live here.
//
//   object:    objectFingerprintV2(v1Fingerprint, nsId) = FNV-1a-64 over the v1 fingerprint (u64 LE) then the
//              nsId (u32 LE).  The same definition in two namespaces hashes differently.
//   namespace: nsFingerprint = FNV-1a-64 over 'N', nsId (u32), name (u32 length + bytes), the 256-byte
//              NamespaceLimits block (rune/protocol/NamespaceLimits.h, wire order) and the state tag (u8).
//   deleted namespace: nsTombstoneFingerprint = FNV-1a-64 over 'X', nsId (u32), name (u32 length + bytes),
//              finalVersion (u32) — what a NamespaceDeleted event carries.
#include <rune/catalog/CatalogEvent.h>
#include <rune/protocol/NamespaceLimits.h>

#include <cstdint>
#include <string_view>

namespace rune::catalog {

[[nodiscard]] uint64_t objectFingerprintV2(uint64_t v1Fingerprint, uint32_t nsId) noexcept;
[[nodiscard]] uint64_t nsFingerprint(uint32_t nsId, std::string_view name, const rune::protocol::NamespaceLimits& limits,
                                     NsStateTag state) noexcept;

[[nodiscard]] uint64_t nsTombstoneFingerprint(uint32_t nsId, std::string_view name, uint32_t finalVersion) noexcept;

} // namespace rune::catalog
