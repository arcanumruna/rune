// libs/view/include/rune/view/TxnSupersede.h
//
// P11-3a on every fold path (Task 8): at its commit marker M a transaction T's records on key x are all skipped when a record of
// another writer on x lies above last_T(x), T's last record on x, and below M, and was applied before M (a plain record, or one of a
// transaction that closed before M).  The rule is per key, as the live judge's: T's earlier records on x are not judged on their own.
// The fold notes T's keys of one segment with last_T over the whole span and, per key, the last offset another writer's applied
// record had; the segment's application skips every T record on a key whose other writer is the later.
//
// Worker-owned and reserved once: the slots and the key arena never grow.  A segment ends at the first T record whose key does
// not fit; the next segment is noted from there.  A key longer than the whole arena is never superseded.
#pragma once
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>
#include <vector>

namespace rune::view {

class SupersedeKeys {
public:
    static constexpr std::size_t kSlots      = 1024;   // a power of two; at most half of them used
    static constexpr std::size_t kMaxKeys    = kSlots / 2;
    static constexpr std::size_t kArenaBytes = 64u << 10;

    SupersedeKeys() : slots_(kSlots), arena_(kArenaBytes), used_(kMaxKeys) {}

    void clear() noexcept {
        for (std::size_t i = 0; i < count_; ++i) slots_[used_[i]] = Slot{};
        count_ = 0;
        arenaUsed_ = 0;
    }
    /// T's record on `key` at `offset`: a noted key's last_T rises; a new key is noted only while `insertNew` (its segment is open).
    /// False when a new key does not fit (the segment ends here).
    [[nodiscard]] bool noteT(std::string_view key, int64_t offset, bool insertNew) noexcept {
        if (key.size() > kArenaBytes) return true;
        std::size_t i = probe_(key);
        if (slots_[i].used) {
            slots_[i].lastT = std::max(slots_[i].lastT, offset);
            return true;
        }
        if (!insertNew) return true;
        if (count_ == kMaxKeys || arenaUsed_ + key.size() > kArenaBytes) return false;
        std::copy(key.begin(), key.end(), arena_.begin() + static_cast<std::ptrdiff_t>(arenaUsed_));
        slots_[i] = Slot{static_cast<uint32_t>(arenaUsed_), static_cast<uint32_t>(key.size()), true, offset, -1};
        arenaUsed_ += key.size();
        used_[count_++] = static_cast<uint32_t>(i);
        return true;
    }
    /// Another writer's applied record on `key` at `offset` (ignored unless the key is noted).
    void noteOther(std::string_view key, int64_t offset) noexcept {
        if (count_ == 0) return;
        Slot& s = slots_[probe_(key)];
        if (s.used) s.lastOther = std::max(s.lastOther, offset);
    }
    /// T's records on `key` are superseded: another writer's applied record lies above last_T(key).
    [[nodiscard]] bool superseded(std::string_view key) const noexcept {
        const Slot& s = slots_[probe_(key)];
        return s.used && s.lastOther > s.lastT;
    }

private:
    struct Slot {
        uint32_t at = 0, len = 0;
        bool     used = false;
        int64_t  lastT = -1, lastOther = -1;
    };
    [[nodiscard]] std::size_t probe_(std::string_view key) const noexcept {
        std::size_t i = std::hash<std::string_view>{}(key) & (kSlots - 1);
        while (slots_[i].used && std::string_view(arena_.data() + slots_[i].at, slots_[i].len) != key) i = (i + 1) & (kSlots - 1);
        return i;
    }

    std::vector<Slot>     slots_;
    std::vector<char>     arena_;
    std::vector<uint32_t> used_;
    std::size_t           count_ = 0, arenaUsed_ = 0;
};

} // namespace rune::view
