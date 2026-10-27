#pragma once
// NamespaceLimitDefaults.h — the cluster-wide namespace limit defaults (spec §5.2, plan Task 7) and the
// presence-mask resolution every namespace DDL uses: a NamespaceDdl's absent fields come from a base record (the
// defaults for a Create, the namespace's current limits for a SetLimits) at REQUEST time, so the catalog event always
// carries the FULL record (a replay never consults a broker's config).
//
// Control plane only (the admin worker): never on the produce / consume / query path.

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include <rune/protocol/NamespaceLimits.h>
#include <rune/protocol/client/NamespaceMessages.h>

namespace rune::node {

/// The defaults a namespace Create resolves absent limit fields from (NodeConfig `RUNE_NS_DEFAULT_*`, read at boot).
struct NamespaceLimitDefaults {
    rune::protocol::NamespaceLimits limits;
};

/// The full record: field i from `given` when `mask` marks it present, else from `base`.  Fields are resolved on the
/// encoded little-endian layout (28 u64 slots at 8*i, then featureBits u32 at 224); reserved fields come from `base`.
[[nodiscard]] inline rune::protocol::NamespaceLimits
resolveLimits(const std::array<uint8_t, rune::protocol::client::kNamespacePresentMaskBytes>& mask,
              const rune::protocol::NamespaceLimits& given, const rune::protocol::NamespaceLimits& base) noexcept {
    namespace rp = rune::protocol;
    uint8_t g[rp::kNamespaceLimitsBytes];
    uint8_t out[rp::kNamespaceLimitsBytes];
    rp::encodeLimits(given, g);
    rp::encodeLimits(base, out);
    for (std::size_t i = 0; i < rp::kNamespaceLimitFieldCount; ++i) {
        if (!rp::client::namespaceMaskHas(mask, i)) continue;
        const std::size_t off   = i < 28 ? 8 * i : 28 * 8;
        const std::size_t width = i < 28 ? 8 : 4;
        std::memcpy(out + off, g + off, width);
    }
    rp::NamespaceLimits r;
    (void)rp::decodeLimits(out, sizeof(out), r);
    return r;
}

} // namespace rune::node
