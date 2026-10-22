#pragma once
// ObjectIndex.h - every live topic / view id -> (name, kind), from the catalog's effects (authentication Task 12).
//
// Control-thread owned (hard rule 1): fed by the control drain of kA2cCatalogTopicApplied / kA2cCatalogSinkApplied /
// kA2cCatalogTopicDropped (and, at boot, by the startup thread before any thread runs).  A PermissionTable compiles
// against it at login; it answers "is this id live?" for D22.  Cold: one vector slot per id (ids are dense, assigned
// by the catalog), capacity persists.
#include <rune/security/SecurityEvent.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace rune::gateway {

struct ObjectEntry {
    std::string name;
    rune::security::ResourceType type = rune::security::ResourceType::Topic;
};

class ObjectIndex {
public:
    void onTopic(int32_t id, std::string name) { set_(id, std::move(name), rune::security::ResourceType::Topic); }
    void onView(int32_t sinkId, std::string name) { set_(sinkId, std::move(name), rune::security::ResourceType::View); }
    void onDropped(int32_t id) noexcept {
        if (static_cast<uint32_t>(id) < entries_.size()) entries_[static_cast<std::size_t>(id)] = ObjectEntry{};
    }
    /// The live entry of `id`, or nullptr (never created, dropped, internal / negative).
    [[nodiscard]] const ObjectEntry* at(int32_t id) const noexcept {
        if (static_cast<uint32_t>(id) >= entries_.size()) return nullptr;
        const ObjectEntry& e = entries_[static_cast<std::size_t>(id)];
        return e.name.empty() ? nullptr : &e;
    }
    /// The live entry named `name`, or nullptr.  Issue #128: a linear scan - cold (GetTopicSchema only); topic and view
    /// names share one name space, so at most one live entry matches.
    [[nodiscard]] const ObjectEntry* byName(std::string_view name) const noexcept {
        for (const ObjectEntry& e : entries_)
            if (!e.name.empty() && e.name == name) return &e;
        return nullptr;
    }
    template <class F> void forEach(F&& f) const {
        for (std::size_t i = 0; i < entries_.size(); ++i)
            if (!entries_[i].name.empty()) f(static_cast<int32_t>(i), entries_[i]);
    }
    /// One past the highest id ever created.
    [[nodiscard]] int32_t idLimit() const noexcept { return static_cast<int32_t>(entries_.size()); }

private:
    void set_(int32_t id, std::string name, rune::security::ResourceType type) {
        if (id < 0 || name.empty()) return;   // internal ids and nameless objects are never granted by pattern
        if (static_cast<std::size_t>(id) >= entries_.size()) entries_.resize(static_cast<std::size_t>(id) + 1);
        entries_[static_cast<std::size_t>(id)] = ObjectEntry{std::move(name), type};
    }
    std::vector<ObjectEntry> entries_;
};

} // namespace rune::gateway
