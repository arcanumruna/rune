#pragma once
// NsSlotTable.h - the per-node namespace slot table (namespaces Phase 2 Task 8, D22).
//
// Every namespace on a node owns one dense slot, 0..capacity-1 (capacity = RUNE_MAX_NAMESPACES).  Slot 0 is `default`
// (nsId 0) from construction.  A slot is allocated, lowest free first, on the apply of NamespaceCreated and freed on
// the apply of the erase (never on NamespaceDeleted: a DELETING namespace still owns its slot and counts toward the
// capacity).  Slots are node-local; only the nsId is cluster-wide.  Apply order is the same on every node, so the
// numbers usually agree, but nothing relies on it.
//
// The authoritative table lives on the control thread (BrokerNode).  The gateway, every worker and every worker's view
// set keep their OWN copy, fed by NsSlotAssign / NsSlotRelease messages on the existing rings (NsRingMessages.h) - no
// shared memory.  Every later ring message carries (slot, nsId); a receiver whose table holds another nsId in that slot
// drops the message and counts `rune.ns.slot_mismatch` (matches() is the one check).
//
// Hot path (rule 4/5): matches() and nsIdAt() are an array index and a compare, no map, no allocation, no virtual.
// assign() / slotOf() scan the slots linearly: control plane (namespace DDL, login) only.
//
// Single-owner: one instance per owning thread; never shared.

#include <rune/protocol/NamespaceLimits.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace rune::node {

inline constexpr uint16_t kNoNsSlot = 0xFFFF;

class NsSlotTable {
public:
    static constexpr uint32_t kFreeSlot = 0xFFFFFFFFu;   // never a real nsId (nsIds are small, monotonic)
    static constexpr uint32_t kDefaultCapacity = 1024;   // RUNE_MAX_NAMESPACES' default

    explicit NsSlotTable(uint32_t capacity = kDefaultCapacity) { reset(capacity); }

    /// Cold: (re)size to `capacity` (>= 1, <= kNoNsSlot) and leave only `default` at slot 0.  Startup only.
    void reset(uint32_t capacity) {
        if (capacity < 1) capacity = 1;
        if (capacity > kNoNsSlot) capacity = kNoNsSlot;
        nsId_.assign(capacity, kFreeSlot);
        limits_.assign(capacity, rune::protocol::NamespaceLimits{});
        nsId_[0] = 0;
        used_    = 1;
    }

    [[nodiscard]] uint32_t capacity() const noexcept { return static_cast<uint32_t>(nsId_.size()); }
    [[nodiscard]] uint32_t used() const noexcept { return used_; }

    /// The lowest free slot for `nsId`; the slot it already owns when it has one (idempotent); nullopt at capacity.
    [[nodiscard]] std::optional<uint16_t> assign(uint32_t nsId) {
        if (const uint16_t have = slotOf(nsId); have != kNoNsSlot) return have;
        for (uint32_t s = 0; s < nsId_.size(); ++s) {
            if (nsId_[s] != kFreeSlot) continue;
            nsId_[s]  = nsId;
            limits_[s] = rune::protocol::NamespaceLimits{};   // a reused slot never inherits the old owner's limits
            ++used_;
            return static_cast<uint16_t>(s);
        }
        return std::nullopt;
    }

    /// A follower copy: put `nsId` at exactly `slot` (the authoritative table chose it).  False (nothing changed) when
    /// the slot is out of range or already holds another nsId, or `nsId` already sits in another slot.
    bool place(uint16_t slot, uint32_t nsId) {
        if (slot >= nsId_.size()) return false;
        if (nsId_[slot] == nsId) return true;
        if (nsId_[slot] != kFreeSlot) return false;
        if (slotOf(nsId) != kNoNsSlot) return false;
        nsId_[slot]  = nsId;
        limits_[slot] = rune::protocol::NamespaceLimits{};
        ++used_;
        return true;
    }

    /// Frees `slot` iff it holds exactly `nsId` (the pair is checked; slot 0 / `default` is never freed).  False
    /// otherwise - the caller counts it as a mismatch.
    bool release(uint16_t slot, uint32_t nsId) {
        if (slot == 0 || slot >= nsId_.size() || nsId_[slot] != nsId || nsId == kFreeSlot) return false;
        nsId_[slot] = kFreeSlot;
        --used_;
        return true;
    }

    /// The slot holding `nsId`, kNoNsSlot when none.  Linear: login / DDL time only.
    [[nodiscard]] uint16_t slotOf(uint32_t nsId) const noexcept {
        if (nsId == kFreeSlot) return kNoNsSlot;
        for (uint32_t s = 0; s < nsId_.size(); ++s)
            if (nsId_[s] == nsId) return static_cast<uint16_t>(s);
        return kNoNsSlot;
    }

    /// Hot path: does `slot` currently hold `nsId`?  One bounds compare, one load, one compare.
    [[nodiscard]] bool matches(uint16_t slot, uint32_t nsId) const noexcept {
        return slot < nsId_.size() && nsId_[slot] == nsId;
    }
    /// The nsId in `slot`, kFreeSlot when free or out of range.
    [[nodiscard]] uint32_t nsIdAt(uint16_t slot) const noexcept { return slot < nsId_.size() ? nsId_[slot] : kFreeSlot; }

    /// The slot's cached limits (NsSlotAssign carries them).  Out-of-range / free slots read as all-zero (unlimited).
    [[nodiscard]] const rune::protocol::NamespaceLimits& limits(uint16_t slot) const noexcept {
        return slot < limits_.size() ? limits_[slot] : kNoLimits_;
    }
    void setLimits(uint16_t slot, const rune::protocol::NamespaceLimits& l) noexcept {
        if (slot < limits_.size() && nsId_[slot] != kFreeSlot) limits_[slot] = l;
    }

private:
    std::vector<uint32_t>                         nsId_;
    std::vector<rune::protocol::NamespaceLimits>  limits_;
    uint32_t                                      used_ = 0;
    static inline const rune::protocol::NamespaceLimits kNoLimits_{};
};

}  // namespace rune::node
