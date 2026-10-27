#pragma once
// rune/gateway/NsResolve.h — a request's object name -> (nsId, local name), for the control thread (namespaces
// plan Task 10, D7 and D9).
//
//   * An unqualified name is the caller's namespace.  "default/t" is nsId 0.
//   * A namespace the caller may not see — any foreign one for a non-root session, or one that does not exist — is
//     Missing with nsId kHiddenNsId.  The handlers authorize the LOCAL name with the session's own permissions and
//     forward it under kHiddenNsId, so the admin worker's own "no topic named 't'" / "namespace ... does not exist"
//     path answers: a hidden namespace's object gets the same code, the same bytes and the same code path as a
//     missing one (hiddenAsMissing), and no foreign namespace name is ever echoed.
//   * Root (ADMIN ON CLUSTER, or an allow-all dev session) reaches every existing namespace.
//
// Zero allocation: string_views over the request and a heterogeneous registry lookup.
#include <rune/catalog/NamespaceRegistry.h>
#include <rune/coordination/NsScopedId.h>

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace rune::gateway {

/// The nsId a hidden or unknown namespace resolves to.  ApplyV2 never assigns it (it refuses "namespace ids are
/// exhausted" first), so every admin lookup under it misses.
inline constexpr uint32_t kHiddenNsId = std::numeric_limits<uint32_t>::max();

enum class Resolved : uint8_t { Found, Missing, Malformed };

struct Resolution {
    Resolved         r     = Resolved::Malformed;
    uint32_t         nsId  = kHiddenNsId;
    std::string_view local;
    bool operator==(const Resolution&) const = default;
};

/// The resolution of a name in a namespace the caller may not see: answered exactly as a missing object.
[[nodiscard]] constexpr Resolution hiddenAsMissing(std::string_view local) noexcept {
    return Resolution{Resolved::Missing, kHiddenNsId, local};
}

/// `callerNsId` is the session's login namespace, `root` its PermissionTable::isRoot().
[[nodiscard]] Resolution resolveName(uint32_t callerNsId, bool root, const rune::catalog::NamespaceRegistry& reg,
                                     std::string_view qualified) noexcept;

/// Namespaces Phase 2 Task 5 (D19): a client's group / transactional id -> the internal scoped id, written into `out`
/// (cleared first; no allocation when its capacity suffices).  A name that resolves (`r == Found`) lands in the
/// namespace it names (root's "a/g" in `a`; anyone's unqualified "g" in their own); anything else (Missing, Malformed:
/// a non-root "b/g", a legacy name containing '/') is an opaque local id in the SESSION namespace — never another one.
/// The scoping lives here (not in rune_coordination) because it needs resolveName.
inline void scopeClientIdInto(const rune::coordination::NsRequestContext& ctx, std::string_view raw, std::string& out)
{
    if (ctx.reg != nullptr) {
        const Resolution r = resolveName(ctx.sessionNsId, ctx.root, *ctx.reg, raw);
        if (r.r == Resolved::Found) { rune::coordination::scopedIdInto(r.nsId, r.local, out); return; }
    }
    rune::coordination::scopedIdInto(ctx.sessionNsId, raw, out);
}

[[nodiscard]] inline std::string scopeClientId(const rune::coordination::NsRequestContext& ctx, std::string_view raw)
{
    std::string s;
    scopeClientIdInto(ctx, raw, s);
    return s;
}

} // namespace rune::gateway
