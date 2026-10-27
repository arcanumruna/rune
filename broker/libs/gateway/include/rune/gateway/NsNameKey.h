#pragma once
// NsNameKey.h - the (nsId, name) key of every broker-side name index (namespaces plan Task 5, D9).
//
// Names are unique per namespace, not per cluster, so the control-thread ObjectIndex and the admin-thread
// SchemaRegistry / ViewRegistry key their name -> id maps by (nsId, name).  NsNameView is the non-owning form: a
// lookup with a string_view builds no std::string (zero allocation).  NsNameHash / NsNameEq are transparent, so
// unordered_map::find(NsNameView) needs no NsName.
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace rune::gateway {

struct NsName {
    uint32_t    nsId = 0;
    std::string name;
    bool operator==(const NsName&) const = default;
};
struct NsNameView {
    uint32_t         nsId = 0;
    std::string_view name;
};

/// FNV-1a over the name bytes, seeded with the nsId (so the same name in two namespaces hashes apart).
[[nodiscard]] inline std::size_t hashNsName(uint32_t nsId, std::string_view name) noexcept {
    uint64_t h = 1469598103934665603ULL ^ (uint64_t{nsId} * 0x9E3779B97F4A7C15ULL);
    for (const char c : name) {
        h ^= static_cast<uint8_t>(c);
        h *= 1099511628211ULL;
    }
    h ^= h >> 32;
    return static_cast<std::size_t>(h);
}

struct NsNameHash {
    using is_transparent = void;
    std::size_t operator()(const NsName& k) const noexcept { return hashNsName(k.nsId, k.name); }
    std::size_t operator()(NsNameView k) const noexcept { return hashNsName(k.nsId, k.name); }
};
struct NsNameEq {
    using is_transparent = void;
    bool operator()(const NsName& a, const NsName& b) const noexcept { return a.nsId == b.nsId && a.name == b.name; }
    bool operator()(const NsName& a, NsNameView b) const noexcept { return a.nsId == b.nsId && a.name == b.name; }
    bool operator()(NsNameView a, const NsName& b) const noexcept { return a.nsId == b.nsId && a.name == b.name; }
};

} // namespace rune::gateway
