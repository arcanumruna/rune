// src/security/SecurityFingerprint.cpp — see rune/security/SecurityFingerprint.h (frozen field order).
#include <rune/security/SecurityFingerprint.h>
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/codec/Fingerprint.h>

#include <algorithm>
#include <string_view>

namespace rune::security {
namespace {
namespace cdd = rune::codec::detail;
using rune::catalog::detail::fnvBytes;

uint64_t str(uint64_t h, std::string_view s) noexcept { return fnvBytes(h, s.data(), s.size()); }
uint64_t u8(uint64_t h, uint64_t v) noexcept { return cdd::fnv1a_byte(h, static_cast<uint8_t>(v)); }
uint64_t u32(uint64_t h, uint64_t v) noexcept { return cdd::fnv1a_u32le(h, static_cast<uint32_t>(v)); }

uint64_t grant(uint64_t h, const GrantSpec& g) noexcept {
    h = u8(h, static_cast<uint8_t>(g.op));
    h = u8(h, static_cast<uint8_t>(g.resource.type));
    h = u8(h, static_cast<uint8_t>(g.resource.kind));
    return str(h, g.resource.name);
}
uint64_t verifier(uint64_t h, const ScramVerifier& v) noexcept {
    h = fnvBytes(h, v.salt.data(), v.salt.size());
    h = u32(h, v.iterations);
    h = fnvBytes(h, v.storedKey.data(), v.storedKey.size());
    return fnvBytes(h, v.serverKey.data(), v.serverKey.size());
}
} // namespace

uint64_t fingerprintOf(const UserObject& u, const SecurityState& s) noexcept {
    uint64_t h = cdd::kFnvSeed;
    h = u8(h, 'U');
    h = str(h, u.name);
    h = u32(h, u.version);
    h = u8(h, u.enabled ? 1 : 0);
    h = u8(h, u.mustChangePassword ? 1 : 0);
    h = u32(h, u.verifiers.size());
    for (const ScramVerifier& v : u.verifiers) h = verifier(h, v);
    // Role NAMES, sorted — never ids.  (A role id the state does not know hashes as an empty name.)
    std::vector<std::string_view> names;
    names.reserve(u.roles.size());
    for (uint64_t rid : u.roles) { const RoleObject* r = s.roleById(rid); names.push_back(r ? std::string_view(r->name) : std::string_view{}); }
    std::sort(names.begin(), names.end());
    h = u32(h, names.size());
    for (std::string_view n : names) h = str(h, n);
    h = u32(h, u.grants.size());
    for (const GrantSpec& g : u.grants) h = grant(h, g);
    return h;
}

uint64_t fingerprintOf(const RoleObject& r) noexcept {
    uint64_t h = cdd::kFnvSeed;
    h = u8(h, 'R');
    h = str(h, r.name);
    h = u32(h, r.version);
    h = u32(h, r.grants.size());
    for (const GrantSpec& g : r.grants) h = grant(h, g);
    return h;
}

uint64_t fingerprintOf(const Tombstone& t) noexcept {
    uint64_t h = cdd::kFnvSeed;
    h = u8(h, 'D');
    h = u8(h, t.wasRole ? 1 : 0);
    h = str(h, t.name);
    return u32(h, t.finalVersion);
}

} // namespace rune::security
