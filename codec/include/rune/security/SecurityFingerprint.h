#pragma once
// rune/security/SecurityFingerprint.h — the fingerprint of a security object's resulting state
// (auth spec §3).  FNV-1a-64 through rune::catalog::detail::fnvU64 / fnvBytes (every variable-length
// field length-prefixed), and NEVER an id: a user's roles are hashed by NAME (sorted), so two
// clusters holding the same definitions under different objectIds hash equal.  Field order (frozen
// with formatVer 1; golden logs pin the values):
//   user:      'U', name, version, enabled, mustChangePassword, verifier count,
//              each verifier (salt, iterations, storedKey, serverKey), role count + each role name
//              (sorted), grant count + each grant (op, type, kind, name) in std::set order
//   role:      'R', name, version, grant count + each grant
//   tombstone: 'D', wasRole, name, finalVersion
// Widths: single bytes as u8; counts, versions and iterations as u32 LE; byte strings as fnvBytes
// (u32 LE length + bytes).
#include <rune/security/SecurityState.h>

#include <cstdint>

namespace rune::security {

[[nodiscard]] uint64_t fingerprintOf(const UserObject&, const SecurityState&);   // role NAMES, never ids
[[nodiscard]] uint64_t fingerprintOf(const RoleObject&) noexcept;
[[nodiscard]] uint64_t fingerprintOf(const Tombstone&) noexcept;

} // namespace rune::security
