#pragma once
// ObjectIndex.h - every live topic / view id -> (nsId, name, kind), from the catalog's effects (authentication Task 12;
// namespaces Task 5).
//
// Control-thread owned (hard rule 1): fed by the control drain of kA2cCatalogTopicApplied / kA2cCatalogSinkApplied /
// kA2cCatalogTopicDropped (and, at boot, by the startup thread before any thread runs).  A PermissionTable compiles
// against it at login; it answers "is this id live?" for D22.  Cold: one vector slot per id (ids are dense, assigned
// by the catalog), capacity persists.
//
// Names are per namespace: the same name may be live in several namespaces, so the name index is keyed by
// (nsId, name).  Issue #128: byName is an O(1) open-addressing lookup (linear probing, power-of-two table, the
// slot holds the id and the hash; the name is compared in entries_) - it allocates nothing and touches one or two
// cache lines.  Topic and view names share one name space per namespace, so at most one live entry matches a key
// (an AlterView's fresh sink briefly shares the view's name with the sink being retired: the newer id wins until the
// old one is dropped).
#include <rune/gateway/NsNameKey.h>
#include <rune/security/SecurityEvent.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace rune::gateway {

struct ObjectEntry {
    std::string name;
    rune::security::ResourceType type = rune::security::ResourceType::Topic;
    uint32_t    nsId = 0;   // last, so `ObjectEntry{name, type}` keeps meaning the default namespace
};

class ObjectIndex {
public:
    void onTopic(uint32_t nsId, int32_t id, std::string name) {
        set_(nsId, id, std::move(name), rune::security::ResourceType::Topic);
    }
    void onView(uint32_t nsId, int32_t sinkId, std::string name) {
        set_(nsId, sinkId, std::move(name), rune::security::ResourceType::View);
    }
    /// The `default` namespace (nsId 0) forms.
    void onTopic(int32_t id, std::string name) { onTopic(0, id, std::move(name)); }
    void onView(int32_t sinkId, std::string name) { onView(0, sinkId, std::move(name)); }

    void onDropped(int32_t id) noexcept {
        if (static_cast<uint32_t>(id) >= entries_.size()) return;
        ObjectEntry& e = entries_[static_cast<std::size_t>(id)];
        if (e.name.empty()) return;
        unindex_(e.nsId, e.name, id);
        e = ObjectEntry{};
    }
    /// The live entry of `id`, or nullptr (never created, dropped, internal / negative).
    [[nodiscard]] const ObjectEntry* at(int32_t id) const noexcept {
        if (static_cast<uint32_t>(id) >= entries_.size()) return nullptr;
        const ObjectEntry& e = entries_[static_cast<std::size_t>(id)];
        return e.name.empty() ? nullptr : &e;
    }
    /// The id of the live object named `name` in namespace `nsId`, if any.  O(1), zero allocation.
    [[nodiscard]] std::optional<int32_t> byName(uint32_t nsId, std::string_view name) const noexcept {
        if (slots_.empty()) return std::nullopt;
        const uint32_t h = hash32_(nsId, name);
        const std::size_t mask = slots_.size() - 1;
        for (std::size_t i = h & mask;; i = (i + 1) & mask) {
            const Slot& s = slots_[i];
            if (s.id < 0) return std::nullopt;
            if (s.hash != h) continue;
            const ObjectEntry& e = entries_[static_cast<std::size_t>(s.id)];
            if (e.nsId == nsId && e.name == name) return s.id;
        }
    }
    template <class F> void forEach(F&& f) const {
        for (std::size_t i = 0; i < entries_.size(); ++i)
            if (!entries_[i].name.empty()) f(static_cast<int32_t>(i), entries_[i]);
    }
    /// Every live object of namespace `nsId`, in id order.
    template <class F> void forEachInNs(uint32_t nsId, F&& f) const {
        for (std::size_t i = 0; i < entries_.size(); ++i)
            if (!entries_[i].name.empty() && entries_[i].nsId == nsId) f(static_cast<int32_t>(i), entries_[i]);
    }
    /// One past the highest id ever created.
    [[nodiscard]] int32_t idLimit() const noexcept { return static_cast<int32_t>(entries_.size()); }

private:
    struct Slot {
        int32_t  id   = -1;   // -1 = empty
        uint32_t hash = 0;
    };
    [[nodiscard]] static uint32_t hash32_(uint32_t nsId, std::string_view name) noexcept {
        return static_cast<uint32_t>(hashNsName(nsId, name));
    }

    void set_(uint32_t nsId, int32_t id, std::string name, rune::security::ResourceType type) {
        if (id < 0 || name.empty()) return;   // internal ids and nameless objects are never granted by pattern
        if (static_cast<std::size_t>(id) >= entries_.size()) entries_.resize(static_cast<std::size_t>(id) + 1);
        ObjectEntry& e = entries_[static_cast<std::size_t>(id)];
        if (!e.name.empty()) unindex_(e.nsId, e.name, id);   // a re-apply under a different key
        e = ObjectEntry{std::move(name), type, nsId};
        index_(e.nsId, e.name, id);
    }
    void index_(uint32_t nsId, std::string_view name, int32_t id) {
        if ((count_ + 1) * 2 > slots_.size()) grow_();
        const uint32_t h = hash32_(nsId, name);
        const std::size_t mask = slots_.size() - 1;
        for (std::size_t i = h & mask;; i = (i + 1) & mask) {
            Slot& s = slots_[i];
            if (s.id < 0) { s = Slot{id, h}; ++count_; return; }
            const ObjectEntry& o = entries_[static_cast<std::size_t>(s.id)];
            if (s.hash == h && o.nsId == nsId && o.name == name) { s.id = id; return; }   // the newer id wins
        }
    }
    /// Removes the (nsId, name) slot only when it still points at `id` (a newer holder of the name keeps it).
    void unindex_(uint32_t nsId, std::string_view name, int32_t id) noexcept {
        if (slots_.empty()) return;
        const uint32_t h = hash32_(nsId, name);
        const std::size_t mask = slots_.size() - 1;
        std::size_t i = h & mask;
        for (;; i = (i + 1) & mask) {
            const Slot& s = slots_[i];
            if (s.id < 0) return;
            if (s.hash != h) continue;
            const ObjectEntry& o = entries_[static_cast<std::size_t>(s.id)];
            if (o.nsId == nsId && o.name == name) break;
        }
        if (slots_[i].id != id) return;
        // Backward-shift deletion: pull later members of the probe run into the hole.
        std::size_t hole = i;
        for (std::size_t j = (i + 1) & mask; slots_[j].id >= 0; j = (j + 1) & mask) {
            const std::size_t home = slots_[j].hash & mask;
            const bool movable = hole <= j ? (home <= hole || home > j) : (home <= hole && home > j);
            if (movable) { slots_[hole] = slots_[j]; hole = j; }
        }
        slots_[hole] = Slot{};
        --count_;
    }
    void grow_() {
        std::vector<Slot> old = std::move(slots_);
        slots_.assign(old.empty() ? 16 : old.size() * 2, Slot{});
        const std::size_t mask = slots_.size() - 1;
        for (const Slot& s : old) {
            if (s.id < 0) continue;
            std::size_t i = s.hash & mask;
            while (slots_[i].id >= 0) i = (i + 1) & mask;
            slots_[i] = s;
        }
    }

    std::vector<ObjectEntry> entries_;
    std::vector<Slot>        slots_;    // power-of-two open-addressing table over entries_, load <= 1/2
    std::size_t              count_ = 0;
};

} // namespace rune::gateway
