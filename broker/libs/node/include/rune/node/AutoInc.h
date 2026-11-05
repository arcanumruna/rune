#pragma once
// AutoInc.h — checked writes Part 06: the autoincrement counter of one partition (inactive / scanning / ready, the pure
// batch plan, reservation blocks) and the side window of assigned bases that answers a retried key-0 batch.
#include <rune/codec/PrimType.h>
#include <rune/storage/ProducerStateManager.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <variant>
#include <vector>

namespace rune::node {

inline constexpr uint32_t    kAutoIncrementReserveMax      = 1u << 20;
inline constexpr std::size_t kAssignedBaseSlots            = 32;
inline constexpr std::size_t kAssignedBaseProducersInitial = 1024;
inline constexpr std::size_t kAssignedBaseProducersMax     = 1u << 20;
static_assert(kAssignedBaseSlots == static_cast<std::size_t>(rune::storage::kRetryWindowSize));

[[nodiscard]] constexpr uint64_t effectiveAutoIncMax(rune::codec::PrimType keyType, uint64_t max) noexcept {
    return keyType == rune::codec::PrimType::UInt32 ? std::min<uint64_t>(max, UINT32_MAX) : max;
}

/// The id of counter value n on `partition` of `partitions`.
[[nodiscard]] constexpr uint64_t autoIncId(uint64_t n, int32_t partitions, int32_t partition) noexcept {
    return n * static_cast<uint64_t>(partitions) + static_cast<uint64_t>(partition);
}

struct AutoIncPlan {
    uint64_t firstN          = 0;
    uint32_t count           = 0;
    uint64_t newNextN        = 0;
    bool     reserve         = false;
    uint64_t reserveThroughN = 0;
    bool     exhausted       = false;
};

struct AutoIncInactive {};
struct AutoIncScanning { uint64_t reservedThroughN = 0; uint64_t maxAssignedN = 0; };
struct AutoIncReady    { uint64_t nextN = 1; uint64_t reservedThroughN = 0; };

class AutoIncState {
public:
    /// Cold.  `effectiveMax` is the topic's effective id cap.
    void configure(int32_t partition, int32_t partitions, uint64_t effectiveMax, uint32_t reserve) noexcept {
        partition_  = partition;
        partitions_ = partitions;
        reserve_    = reserve;
        nMax_       = effectiveMax >= static_cast<uint64_t>(partition) ? (effectiveMax - static_cast<uint64_t>(partition)) / static_cast<uint64_t>(partitions) : 0;
    }

    void beginScan() noexcept { st_ = AutoIncScanning{}; }
    void onReservation(uint64_t reservedThroughN) noexcept {
        if (auto* s = std::get_if<AutoIncScanning>(&st_)) s->reservedThroughN = std::max(s->reservedThroughN, reservedThroughN);
    }
    void onAssignedThrough(uint64_t n) noexcept {
        if (auto* s = std::get_if<AutoIncScanning>(&st_)) s->maxAssignedN = std::max(s->maxAssignedN, n);
    }
    /// Moves to Ready{R + 1, R}; returns the burned gap R - max(maxAssignedN, burnedThroughN) (0 when not positive).  An id
    /// is counted burned once: a later scan over the same reservation, or over ids an append failure burned, adds nothing.
    [[nodiscard]] uint64_t finishScan() noexcept {
        const auto* s = std::get_if<AutoIncScanning>(&st_);
        if (s == nullptr) return 0;
        const uint64_t r = s->reservedThroughN;
        const uint64_t counted = std::max(s->maxAssignedN, burnedThroughN_);
        const uint64_t burned = r > counted ? r - counted : 0;
        burnedThroughN_ = std::max(burnedThroughN_, r);
        st_ = AutoIncReady{r + 1, r};
        return burned;
    }
    /// The ids through `n` are counted burned (an append failure after they were taken).
    void burnedThrough(uint64_t n) noexcept { burnedThroughN_ = std::max(burnedThroughN_, n); }
    void deactivate() noexcept { st_ = AutoIncInactive{}; }
    [[nodiscard]] bool ready() const noexcept { return std::holds_alternative<AutoIncReady>(st_); }

    /// Pure.  `importMaxN` is the highest n an import in the batch carries (0 = none).
    [[nodiscard]] AutoIncPlan plan(uint32_t keyZeroCount, uint64_t importMaxN) const noexcept {
        const auto* s = std::get_if<AutoIncReady>(&st_);
        if (s == nullptr) return {};
        AutoIncPlan p;
        const uint64_t lastN = s->nextN + keyZeroCount - 1;
        const uint64_t highestN = std::max(keyZeroCount > 0 ? lastN : 0, importMaxN);
        p.firstN          = keyZeroCount > 0 ? s->nextN : 0;
        p.count           = keyZeroCount;
        p.exhausted       = keyZeroCount > 0 && lastN > nMax_;
        p.reserveThroughN = std::min(nMax_, std::max(highestN, s->reservedThroughN + reserve_));
        p.reserve         = !p.exhausted && highestN > s->reservedThroughN && p.reserveThroughN > s->reservedThroughN;   // the caller rejects an importMaxN above nMax
        p.newNextN        = std::max(s->nextN + keyZeroCount, importMaxN + 1);
        return p;
    }

    void commitReservation(uint64_t reservedThroughN) noexcept {
        if (auto* s = std::get_if<AutoIncReady>(&st_)) s->reservedThroughN = std::max(s->reservedThroughN, reservedThroughN);
    }
    void commitNext(uint64_t newNextN) noexcept {
        if (auto* s = std::get_if<AutoIncReady>(&st_)) s->nextN = std::max(s->nextN, newNextN);
    }

    [[nodiscard]] uint64_t nextN() const noexcept {
        const auto* s = std::get_if<AutoIncReady>(&st_);
        return s != nullptr ? s->nextN : 0;
    }
    [[nodiscard]] uint64_t reservedThroughN() const noexcept {
        if (const auto* s = std::get_if<AutoIncReady>(&st_)) return s->reservedThroughN;
        if (const auto* s = std::get_if<AutoIncScanning>(&st_)) return s->reservedThroughN;
        return 0;
    }
    [[nodiscard]] uint64_t nMax() const noexcept { return nMax_; }
    [[nodiscard]] int32_t  partition() const noexcept { return partition_; }
    [[nodiscard]] int32_t  partitions() const noexcept { return partitions_; }

private:
    std::variant<AutoIncInactive, AutoIncScanning, AutoIncReady> st_;
    uint64_t burnedThroughN_ = 0;   // every id through it is already in idsBurned; kept across scans
    int32_t  partition_  = 0;
    int32_t  partitions_ = 1;
    uint32_t reserve_    = 0;
    uint64_t nMax_       = 0;
};

/// The last kAssignedBaseSlots key-0 batches' assigned bases per producer, keyed by batch base offset.  An open-addressed
/// table without deletion (clear() resets it); `record` never allocates.
class AssignedBaseWindow {
public:
    void reserve(std::size_t producers) { if (producers > slots_.size()) rehash_(producers); }
    void clear() noexcept {
        for (Slot& s : slots_) s = Slot{};
        used_ = 0;
    }

    /// True when a batch of `producerId` can be recorded: it has a slot or a free one remains.
    [[nodiscard]] bool canAccept(int64_t producerId) const noexcept { return used_ < slots_.size() || locate_(producerId) != nullptr; }

    /// Key-0 batches only.  Precondition: producerId >= 0, canAccept(producerId), and reserve() has been called.
    void record(int64_t producerId, int64_t baseOffset, uint64_t assignedBase) noexcept { put_(producerId, baseOffset, assignedBase, true); }

    /// Cold: a scan refill; grows when full.  The provenance of the batch is unknown.
    void recordScanned(int64_t producerId, int64_t baseOffset, uint64_t assignedBase) {
        if (!canAccept(producerId) && slots_.size() < kAssignedBaseProducersMax) rehash_(slots_.size() * 2);
        if (canAccept(producerId)) put_(producerId, baseOffset, assignedBase, false);
    }

    /// 0 = unknown.
    [[nodiscard]] uint64_t find(int64_t producerId, int64_t baseOffset) const noexcept {
        const Entry* e = entry_(producerId, baseOffset);
        return e != nullptr ? e->assignedBase : 0;
    }
    /// True only for an entry that record() wrote.
    [[nodiscard]] bool isKeyZeroBatch(int64_t producerId, int64_t baseOffset) const noexcept {
        const Entry* e = entry_(producerId, baseOffset);
        return e != nullptr && e->keyZero;
    }

    [[nodiscard]] std::size_t producers() const noexcept { return used_; }
    /// P6-8: the producer load passed 75 % and the window can still grow.
    [[nodiscard]] bool wantsGrowth() const noexcept { return used_ * 4 >= slots_.size() * 3 && slots_.size() < kAssignedBaseProducersMax; }
    [[nodiscard]] std::size_t capacity() const noexcept { return slots_.size(); }

    /// Cold.
    void grow(std::size_t producers) { reserve(producers); }

private:
    struct Entry { int64_t baseOffset = 0; uint64_t assignedBase = 0; bool keyZero = false; };
    struct Slot {
        int64_t  producerId = -1;
        uint32_t head       = 0;
        uint32_t count      = 0;
        std::array<Entry, kAssignedBaseSlots> entries{};
    };

    [[nodiscard]] std::size_t home_(int64_t producerId) const noexcept {
        return static_cast<std::size_t>((static_cast<uint64_t>(producerId) * 0x9E3779B97F4A7C15ull) >> 32) & (slots_.size() - 1);
    }
    /// The producer's slot, or nullptr.
    [[nodiscard]] const Slot* locate_(int64_t producerId) const noexcept {
        if (slots_.empty()) return nullptr;
        for (std::size_t i = 0, at = home_(producerId); i < slots_.size(); ++i, at = (at + 1) & (slots_.size() - 1)) {
            if (slots_[at].producerId == producerId) return &slots_[at];
            if (slots_[at].producerId < 0) return nullptr;
        }
        return nullptr;
    }
    [[nodiscard]] const Entry* entry_(int64_t producerId, int64_t baseOffset) const noexcept {
        const Slot* s = locate_(producerId);
        if (s == nullptr) return nullptr;
        for (uint32_t i = 0; i < s->count; ++i)
            if (s->entries[i].baseOffset == baseOffset) return &s->entries[i];
        return nullptr;
    }
    void put_(int64_t producerId, int64_t baseOffset, uint64_t assignedBase, bool keyZero) noexcept {
        std::size_t at = home_(producerId);
        while (slots_[at].producerId != producerId && slots_[at].producerId >= 0) at = (at + 1) & (slots_.size() - 1);
        Slot& s = slots_[at];
        if (s.producerId < 0) { s.producerId = producerId; ++used_; }
        s.entries[s.head] = Entry{baseOffset, assignedBase, keyZero};
        s.head = (s.head + 1) % kAssignedBaseSlots;
        s.count = std::min<uint32_t>(s.count + 1, kAssignedBaseSlots);
    }
    void rehash_(std::size_t producers) {
        std::size_t cap = 1;
        while (cap < producers) cap <<= 1;
        std::vector<Slot> old(cap);
        old.swap(slots_);
        for (const Slot& s : old) {
            if (s.producerId < 0) continue;
            std::size_t at = home_(s.producerId);
            while (slots_[at].producerId >= 0) at = (at + 1) & (slots_.size() - 1);
            slots_[at] = s;
        }
    }

    std::vector<Slot> slots_;
    std::size_t       used_ = 0;
};

struct AutoIncStats {
    uint64_t idsBurned = 0, reservationsAppended = 0, keyZeroBatches = 0, importsMovedCounter = 0,
             windowMisses = 0, windowFullRefusals = 0, assignedKeyExists = 0;
};

}  // namespace rune::node
