// src/catalog/CatalogFingerprintV2.cpp — see CatalogFingerprintV2.h.
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/CatalogFingerprintV2.h>
#include <rune/codec/Fingerprint.h>

namespace rune::catalog {

namespace cdd = rune::codec::detail;

uint64_t objectFingerprintV2(uint64_t v1Fingerprint, uint32_t nsId) noexcept {
    uint64_t h = cdd::kFnvSeed;
    h = detail::fnvU64(h, v1Fingerprint);
    return cdd::fnv1a_u32le(h, nsId);
}

uint64_t nsFingerprint(uint32_t nsId, std::string_view name, const rune::protocol::NamespaceLimits& limits,
                       NsStateTag state) noexcept {
    uint64_t h = cdd::kFnvSeed;
    h = cdd::fnv1a_byte(h, 'N');
    h = cdd::fnv1a_u32le(h, nsId);
    h = detail::fnvBytes(h, name.data(), name.size());
    uint8_t block[rune::protocol::kNamespaceLimitsBytes];
    rune::protocol::encodeLimits(limits, block);
    for (const uint8_t b : block) h = cdd::fnv1a_byte(h, b);
    return cdd::fnv1a_byte(h, static_cast<uint8_t>(state));
}

uint64_t nsTombstoneFingerprint(uint32_t nsId, std::string_view name, uint32_t finalVersion) noexcept {
    uint64_t h = cdd::kFnvSeed;
    h = cdd::fnv1a_byte(h, 'X');
    h = cdd::fnv1a_u32le(h, nsId);
    h = detail::fnvBytes(h, name.data(), name.size());
    return cdd::fnv1a_u32le(h, finalVersion);
}

} // namespace rune::catalog
