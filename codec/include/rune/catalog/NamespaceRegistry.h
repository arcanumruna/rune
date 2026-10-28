#pragma once
// rune/catalog/NamespaceRegistry.h — the namespaces of a catalog (plan D1, D5), owned by CatalogState and mutated
// only by the formatVer-2 apply rules (ApplyV2).  nsId 0 `default` is predefined, always present, never carried as
// an event, never deleted.  Every other namespace is created by a NamespaceCreated event whose nsId must equal
// nextNsId(); ids are never reused (maxEverNsId survives Deleted).
//
// A namespace's state is a rich-state variant, each alternative holding exactly what it needs:
//   NsActive                      the only state a namespace is created in
//   NsSuspended{mode}             ReadOnly or RejectAll
//   NsDeleting{pendingTopics, pendingViews, stepMask}
//                                 what is still left to clean up
// Lifecycle: ACTIVE <-> SUSPENDED(mode), SUSPENDED(RO) <-> SUSPENDED(RA),
//   ACTIVE | SUSPENDED --NamespaceDeleted--> DELETING --(objects dropped + 4 steps)--> erased.
// DELETING is entered only by NamespaceDeleted (never by NamespaceStateChanged), which drops the name from the name
// index at once: a DELETING namespace is reachable by id only and its name is reusable.  The record is erased when
// every pending object is dropped and all four NamespaceDeleteStep bits are recorded.
// Plan D18 lists pendingSchemas; schemas live inside their topic, so dropping the topic drops them.
#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/NamespaceDeleteStep.h>
#include <rune/catalog/NsName.h>
#include <rune/protocol/NamespaceLimits.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <set>
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
/// A DELETING namespace: the objectIds still live in it and the cleanup steps recorded so far.
struct NsDeleting {
    std::set<uint64_t> pendingTopics, pendingViews;   // objectIds still live in the namespace
    uint8_t            stepMask = 0;                  // NsDeleteStepBit bits recorded so far
    [[nodiscard]] bool complete() const noexcept {
        return pendingTopics.empty() && pendingViews.empty() && stepMask == kNsDeleteAllSteps;
    }
    bool operator==(const NsDeleting&) const = default;
};
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
    /// Namespaces that exist: excludes erased ones, includes DELETING ones (and `default`); they count toward limits.
    [[nodiscard]] std::size_t liveCount() const noexcept { return byId_.size(); }
    /// In nsId order.
    template <class F> void forEach(F&& f) const { for (const auto& [id, rec] : byId_) f(rec); }
    /// FNV fold of every NamespaceDeleted tombstone fingerprint, in apply order (0 = none ever): the digest's trace of
    /// deleted namespaces, as object tombstones are for dropped objects.
    [[nodiscard]] uint64_t tombstoneFold() const noexcept { return tombstoneFold_; }

private:
    friend struct ApplyV2;
    friend class NamespaceMirror;
    std::map<uint32_t, NamespaceRecord>            byId_;
    std::map<std::string, uint32_t, std::less<>>   byName_;
    uint32_t                                       nextNsId_ = 1;
    uint64_t                                       tombstoneFold_ = 0;
};

/// A read-only COPY of a catalog's namespaces kept by a thread that does not own the CatalogState (namespaces Task 9:
/// the control thread's, for the login lookup).  Seeded from the booted registry on the startup thread, then fed the
/// applier's namespaceUpserted effects over the admin -> control ring.  An upsert is idempotent and never moves a
/// namespace back: a record whose version is below the held one is ignored (a re-posted boot effect behind a live one).
/// `default` is predefined, so a bare login resolves before any effect arrives.
class NamespaceMirror {
public:
    NamespaceMirror() = default;
    explicit NamespaceMirror(const NamespaceRegistry& booted) : reg_(booted) {}

    [[nodiscard]] const NamespaceRegistry& registry() const noexcept { return reg_; }

    /// Inserts or replaces `rec` (keyed by nsId; a renamed record drops its old name).  False = ignored (older version).
    bool upsert(const NamespaceRecord& rec) {
        auto it = reg_.byId_.find(rec.nsId);
        if (it != reg_.byId_.end()) {
            if (rec.version < it->second.version) return false;
            if (it->second.name != rec.name) dropName(it->second.name, rec.nsId);
            it->second = rec;
        } else {
            reg_.byId_.emplace(rec.nsId, rec);
        }
        if (std::holds_alternative<NsDeleting>(rec.state)) dropName(rec.name, rec.nsId);   // by id only; the name may be reused
        else reg_.byName_.insert_or_assign(rec.name, rec.nsId);
        if (rec.nsId >= reg_.nextNsId_) reg_.nextNsId_ = rec.nsId + 1;
        return true;
    }
    /// Removes `nsId` (its name too, if the name is still its).  False = it was not held.
    bool erase(uint32_t nsId) {
        const auto it = reg_.byId_.find(nsId);
        if (it == reg_.byId_.end()) return false;
        dropName(it->second.name, nsId);
        reg_.byId_.erase(it);
        return true;
    }

private:
    /// Erases `name` from the name index only when it maps to `nsId` (a recreated namespace may own it now).
    void dropName(const std::string& name, uint32_t nsId) {
        const auto n = reg_.byName_.find(name);
        if (n != reg_.byName_.end() && n->second == nsId) reg_.byName_.erase(n);
    }
    NamespaceRegistry reg_;
};

} // namespace rune::catalog
