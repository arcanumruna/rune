#pragma once
// PermissionPool.h - issue #259 (X108): ONE compiled PermissionTable per distinct effective grant set, shared by every
// session whose principal has that set (a role's members, a user's sessions).
//
// Before: every login and every live refresh compiled the session's own table - O(objects x grants) PER SESSION (10k
// sessions of one role x 100k topics = 1e9 prefix compares for one GRANT, on the control thread).  Now a login or a
// refresh costs one effectiveGrants() + one hash lookup; a set is compiled once, when it first appears, and a new or
// dropped topic / view updates each live TABLE once (O(tables x grants)), never each session.
//
// The key is the sorted, unique grant list itself (exact compare on a hash hit: a collision never shares a table).
// Each entry is ref-counted by its holders (PermissionRef): plain counts, control thread only (hard rule 1 - no atomics);
// an entry whose last holder left is freed and its id reused.  Ids are dense and bounded (kMaxTables): they name the
// table on the shard workers too (PermissionShipper ships a table once and binds sessions to it).
//
// Cold (login / refresh / DDL rate).  The hot-path check stays PermissionTable::allows - one bounds check + one load,
// through one pointer the session already holds.
#include <rune/gateway/ObjectIndex.h>
#include <rune/gateway/PermissionTable.h>
#include <rune/security/Permissions.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace rune::gateway {

class PermissionPool;

/// The deny-everything table a ref without a table of its own points at (never null: the hot path is ONE load).
[[nodiscard]] inline const PermissionTable* denyAllPermissionTable() noexcept {
    static const PermissionTable kDeny = PermissionTable::denyAll();
    return &kDeny;
}

/// A session's hold on one pooled table (or, test seams only, a table of its own).  Copy = one more holder.
class PermissionRef {
public:
    PermissionRef() = default;
    /// Test seam / unpooled: a table owned by this ref alone (shipped as nothing: never used on a shard worker).
    PermissionRef(PermissionTable owned)   // NOLINT(google-explicit-constructor): AuthAuthenticated{..., table, ...}
        : owned_(std::make_shared<const PermissionTable>(std::move(owned))) { table_ = owned_.get(); }
    PermissionRef(const PermissionRef& o);
    PermissionRef(PermissionRef&& o) noexcept
        : pool_(std::exchange(o.pool_, nullptr)), id_(o.id_), table_(std::exchange(o.table_, denyAllPermissionTable())),
          owned_(std::move(o.owned_)) {}
    PermissionRef& operator=(PermissionRef o) noexcept { swap(o); return *this; }
    ~PermissionRef();
    void swap(PermissionRef& o) noexcept {
        std::swap(pool_, o.pool_); std::swap(id_, o.id_); std::swap(table_, o.table_); owned_.swap(o.owned_);
    }

    /// The hot path (every produce / subscribe / fetch authorization): ONE load, inline, never null.
    [[nodiscard]] const PermissionTable& table() const noexcept { return *table_; }
    const PermissionTable* operator->() const noexcept { return &table(); }
    /// The pool id (what a shard worker knows the table by); kNoId = not pooled.
    static constexpr uint32_t kNoId = UINT32_MAX;
    [[nodiscard]] uint32_t id() const noexcept { return pool_ != nullptr ? id_ : kNoId; }

private:
    friend class PermissionPool;
    PermissionRef(PermissionPool* pool, uint32_t id, const PermissionTable* t) noexcept : pool_(pool), id_(id), table_(t) {}
    PermissionPool*                          pool_  = nullptr;
    uint32_t                                 id_    = 0;
    const PermissionTable*                   table_ = denyAllPermissionTable();   // never null   // the pool entry's table (stable: entries never move)
    std::shared_ptr<const PermissionTable>   owned_;            // test seam only (never on a pooled ref)
};

class PermissionPool {
public:
    /// At most this many distinct live grant sets (the shard workers' table store has the same bound).
    static constexpr uint32_t kMaxTables = 4096;
    /// What the shard workers must learn, in order: an entry's first holder appeared (Created: ship the table) or
    /// its last left (Freed: drop it).  Queued, never a callback (a ref may die anywhere, e.g. inside a session close);
    /// the gateway drains them before it binds a session and every duty cycle.
    enum class Event : uint8_t { Created, Freed };
    struct PendingEvent { Event event; uint32_t id; };

    PermissionPool() { entries_.reserve(64); free_.reserve(kMaxTables); events_.reserve(64); }
    PermissionPool(const PermissionPool&) = delete;
    PermissionPool& operator=(const PermissionPool&) = delete;
    /// FIFO; `f(event, id, table)` - for Created the entry's CURRENT table (its id may already be freed again: then
    /// the Freed that follows drops it).  Cold.
    template <class F> void drainEvents(F&& f) {
        for (std::size_t i = 0; i < events_.size(); ++i) f(events_[i].event, events_[i].id, entries_[events_[i].id]->table);
        events_.clear();
    }
    [[nodiscard]] bool hasEvents() const noexcept { return !events_.empty(); }

    /// The shared table of `grants` (sorted, unique - rune::security::effectiveGrants), compiled against `index` only
    /// when no live entry has that exact set.  Full pool (kMaxTables distinct live sets): a deny-all ref (fail closed).
    [[nodiscard]] PermissionRef acquire(std::vector<rune::security::GrantSpec> grants, const ObjectIndex& index);

    /// A topic / view appeared / disappeared: every LIVE table once.  `f(id, table, beforeWrite, afterWrite)` sees
    /// each table's WRITE bit of `objectId` before and after (the shipping hook patches the ones that changed).
    template <class F> void onObjectCreated(int32_t objectId, const ObjectEntry& e, F&& f) {
        for (uint32_t i = 0; i < entries_.size(); ++i) {
            Entry& en = *entries_[i];
            if (en.refs == 0) continue;
            const bool before = en.table.allows(objectId, kWriteBit);
            en.table.onObjectCreated(objectId, e);
            f(i, en.table, before, en.table.allows(objectId, kWriteBit));
        }
    }
    template <class F> void onObjectDropped(int32_t objectId, F&& f) {
        for (uint32_t i = 0; i < entries_.size(); ++i) {
            Entry& en = *entries_[i];
            if (en.refs == 0) continue;
            const bool before = en.table.allows(objectId, kWriteBit);
            en.table.onObjectDropped(objectId);
            f(i, en.table, before, false);
        }
    }

    [[nodiscard]] std::size_t liveTables() const noexcept { return live_; }
    [[nodiscard]] uint64_t compiles() const noexcept { return compiles_; }   ///< tables compiled so far (tests, bench)
    [[nodiscard]] uint32_t refsOf(uint32_t id) const noexcept { return id < entries_.size() ? entries_[id]->refs : 0; }

private:
    friend class PermissionRef;
    static constexpr uint8_t kWriteBit = rune::security::opBit(rune::security::Operation::Write);
    struct Entry {
        std::vector<rune::security::GrantSpec> key;
        uint64_t        hash = 0;
        PermissionTable table;
        uint32_t        refs = 0;
    };
    static uint64_t hashOf_(std::span<const rune::security::GrantSpec>) noexcept;
    void addRef_(uint32_t id) noexcept { ++entries_[id]->refs; }
    void release_(uint32_t id) noexcept;

    std::vector<std::unique_ptr<Entry>>         entries_;   // by id; an Entry never moves (refs hold &table)
    std::vector<uint32_t>                       free_;      // ids of freed entries (reused first)
    std::unordered_multimap<uint64_t, uint32_t> byHash_;    // live entries only
    std::size_t                                 live_ = 0;
    uint64_t                                    compiles_ = 0;
    std::vector<PendingEvent>                   events_;
    PermissionTable                             denyAll_ = PermissionTable::denyAll();
};

} // namespace rune::gateway
