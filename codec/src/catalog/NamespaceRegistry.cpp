// src/catalog/NamespaceRegistry.cpp — see NamespaceRegistry.h.
#include <rune/catalog/NamespaceRegistry.h>
#include <rune/catalog/NsName.h>

namespace rune::catalog {

NamespaceRegistry::NamespaceRegistry(const rune::protocol::NamespaceLimits& defaultNsLimits) {
    NamespaceRecord d;
    d.nsId = kDefaultNsId;
    d.name = std::string(kDefaultNsName);
    d.limits = defaultNsLimits;
    d.state = NsActive{};
    d.version = 0;
    d.fingerprint = 0;   // predefined: not fingerprinted until a NamespaceLimitsSet changes it
    byName_.emplace(d.name, d.nsId);
    byId_.emplace(d.nsId, std::move(d));
}

const NamespaceRecord* NamespaceRegistry::byId(uint32_t nsId) const {
    const auto it = byId_.find(nsId);
    return it == byId_.end() ? nullptr : &it->second;
}

const NamespaceRecord* NamespaceRegistry::byName(std::string_view name) const {
    const auto it = byName_.find(name);
    return it == byName_.end() ? nullptr : byId(it->second);
}

} // namespace rune::catalog
