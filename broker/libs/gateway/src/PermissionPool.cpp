// PermissionPool.cpp - see rune/gateway/PermissionPool.h.
#include <rune/gateway/PermissionPool.h>

#include <spdlog/spdlog.h>

#include <algorithm>

namespace rune::gateway {

namespace rsec = rune::security;

PermissionRef::PermissionRef(const PermissionRef& o) : pool_(o.pool_), id_(o.id_), table_(o.table_), owned_(o.owned_) {
    if (pool_ != nullptr) pool_->addRef_(id_);
}

PermissionRef::~PermissionRef() {
    if (pool_ != nullptr) pool_->release_(id_);
}

uint64_t PermissionPool::hashOf_(std::span<const rsec::GrantSpec> grants, uint32_t nsId) noexcept {
    uint64_t h = 0xcbf29ce484222325ULL;   // FNV-1a over every field of every grant
    auto mix = [&h](uint64_t v) { h ^= v; h *= 0x100000001b3ULL; };
    mix(nsId);
    for (const rsec::GrantSpec& g : grants) {
        mix(static_cast<uint64_t>(g.op));
        mix(static_cast<uint64_t>(g.resource.type));
        mix(static_cast<uint64_t>(g.resource.kind));
        mix(std::hash<std::string_view>{}(g.resource.name));
    }
    return h;
}

PermissionRef PermissionPool::acquire(std::vector<rsec::GrantSpec> grants, uint32_t nsId, const ObjectIndex& index,
                                     std::string_view nsName) {
    const uint64_t h = hashOf_(grants, nsId);
    for (auto [it, end] = byHash_.equal_range(h); it != end; ++it) {
        Entry& e = *entries_[it->second];
        if (e.nsId == nsId && e.key == grants) {   // exact: a hash collision never shares a table
            ++e.refs;
            return PermissionRef(this, it->second, &e.table);
        }
    }
    uint32_t id = 0;
    if (!free_.empty()) {
        id = free_.back();
        free_.pop_back();
    } else if (entries_.size() < kMaxTables) {
        id = static_cast<uint32_t>(entries_.size());
        entries_.push_back(std::make_unique<Entry>());
    } else {
        spdlog::error("auth: {} distinct live grant sets (the limit) - a session with a new one is denied everything",
                      live_);
        return PermissionRef{};   // deny-all, never pooled (fail closed)
    }
    Entry& e = *entries_[id];
    e.table = PermissionTable::compile(grants, nsId, index, nsName);
    ++compiles_;
    e.key   = std::move(grants);
    e.hash  = h;
    e.nsId  = nsId;
    e.refs  = 1;
    ++live_;
    byHash_.emplace(h, id);
    events_.push_back({Event::Created, id});
    return PermissionRef(this, id, &e.table);
}

void PermissionPool::release_(uint32_t id) noexcept {
    Entry& e = *entries_[id];
    if (--e.refs != 0) return;
    for (auto [it, end] = byHash_.equal_range(e.hash); it != end; ++it)
        if (it->second == id) { byHash_.erase(it); break; }
    --live_;
    events_.push_back({Event::Freed, id});
    e.key.clear();
    e.table = PermissionTable::denyAll();
    free_.push_back(id);
}

} // namespace rune::gateway
