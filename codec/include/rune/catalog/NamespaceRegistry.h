#pragma once
// rune/catalog/NamespaceRegistry.h — the namespaces of a catalog (plan D1, D5), owned by CatalogState and mutated
// only by the formatVer-2 apply rules (ApplyV2).  nsId 0 `default` is predefined, always present, never carried as
// an event, never deleted.  Every other namespace is created by a NamespaceCreated event whose nsId must equal
// nextNsId(); ids are never reused (maxEverNsId survives Deleted).
//
// A namespace's state is a rich-state variant, each alternative holding exactly what it needs:
//   NsActive                      the only state a namespace is created in
//   NsSuspended{mode}             ReadOnly or RejectAll (ACTIVE <-> SUSPENDED)
//   NsDeleting                    ACTIVE | SUSPENDED -> DELETING; only a DELETING namespace may be Deleted
#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/NsName.h>
#include <rune/protocol/NamespaceLimits.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <variant>

namespace rune::catalog {

/// The key of every per-namespace name index: (nsId, local name).
struct NsKey {
    uint32_t    nsId = 0;
    std::string name;
    auto operator<=>(const NsKey&) const = default;
};
/// A non-owning NsKey, for lookups without building a std::string.
struct NsKeyView {
    uint32_t         nsId;
    std::string_view name;
};
/// Transparent ordering over NsKey / NsKeyView (same order as NsKey's <=>: nsId, then name bytes).
struct NsKeyLess {
    using is_transparent = void;
    template <class A, class B> bool operator()(const A& a, const B& b) const noexcept {
        if (a.nsId != b.nsId) return a.nsId < b.nsId;
        return std::string_view(a.name) < std::string_view(b.name);
    }
};

struct NsActive {};
struct NsSuspended { NsStateTag mode = NsStateTag::SuspendedReadOnly; };   // SuspendedReadOnly | SuspendedRejectAll
struct NsDeleting {};
using NsState = std::variant<NsActive, NsSuspended, NsDeleting>;

[[nodiscard]] constexpr NsStateTag tagOf(const NsState& s) noexcept {
    if (std::holds_alternative<NsActive>(s)) return NsStateTag::Active;
    if (const auto* x = std::get_if<NsSuspended>(&s)) return x->mode;
    return NsStateTag::Deleting;
}
[[nodiscard]] constexpr NsState stateOf(NsStateTag t) noexcept {
    switch (t) {
    case NsStateTag::Active:             return NsActive{};
    case NsStateTag::SuspendedReadOnly:
    case NsStateTag::SuspendedRejectAll: return NsSuspended{t};
    case NsStateTag::Deleting:           return NsDeleting{};
    }
    return NsActive{};   // unreachable: the tag was validated
}

struct NamespaceRecord {
    uint32_t                          nsId        = 0;
    std::string                       name;
    rune::protocol::NamespaceLimits   limits;
    NsState                           state;
    uint32_t                          version     = 0;   // 0 = the predefined default, never changed; Created = 1
    uint64_t                          fingerprint = 0;   // nsFingerprint of this record; 0 for an untouched default
    bool operator==(const NamespaceRecord&) const = default;
};

struct ApplyV2;   // src/catalog/ApplyV2.cpp — the only mutator

class NamespaceRegistry {
public:
    /// Predefines nsId 0 `default` with the cluster default limits (resolved at boot).
    explicit NamespaceRegistry(const rune::protocol::NamespaceLimits& defaultNsLimits = {});

    [[nodiscard]] const NamespaceRecord* byId(uint32_t nsId) const;
    [[nodiscard]] const NamespaceRecord* byName(std::string_view name) const;
    /// maxEverNsId + 1: never decreases, survives Deleted.
    [[nodiscard]] uint32_t nextNsId() const noexcept { return nextNsId_; }
    /// Namespaces that exist: excludes Deleted, includes Deleting (and `default`).
    [[nodiscard]] std::size_t liveCount() const noexcept { return byId_.size(); }
    /// In nsId order.
    template <class F> void forEach(F&& f) const { for (const auto& [id, rec] : byId_) f(rec); }

private:
    friend struct ApplyV2;
    std::map<uint32_t, NamespaceRecord>            byId_;
    std::map<std::string, uint32_t, std::less<>>   byName_;
    uint32_t                                       nextNsId_ = 1;
};

} // namespace rune::catalog
