#include <rune/gateway/NsResolve.h>

#include <rune/catalog/NsName.h>

namespace rune::gateway {

Resolution resolveName(uint32_t callerNsId, bool root, const rune::catalog::NamespaceRegistry& reg,
                       std::string_view qualified) noexcept
{
    const auto q = rune::catalog::parseQualified(qualified);
    if (!q) return Resolution{Resolved::Malformed, kHiddenNsId, {}};
    if (q->ns.empty()) return Resolution{Resolved::Found, callerNsId, q->local};
    uint32_t nsId = rune::catalog::kDefaultNsId;
    if (q->ns != rune::catalog::kDefaultNsName) {
        const rune::catalog::NamespaceRecord* rec = reg.byName(q->ns);
        if (rec == nullptr) return hiddenAsMissing(q->local);
        nsId = rec->nsId;
    }
    if (nsId != callerNsId && !root) return hiddenAsMissing(q->local);
    return Resolution{Resolved::Found, nsId, q->local};
}

} // namespace rune::gateway
