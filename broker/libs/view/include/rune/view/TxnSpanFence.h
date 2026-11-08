// libs/view/include/rune/view/TxnSpanFence.h
//
// Part 11 (P11-0, P11-9, contract addition 13): the open transactions one view instance's fold has met and not yet applied.
// A fold that meets a transactional batch records {pid, first} and skips it; at the pid's commit marker it reads the pid's
// batches back from [first, M) and applies them, at its abort marker it drops them.  Everything below the fold cursor is
// therefore applied or held in an open span, and a checkpoint at O is valid only while no span straddles O.
// Worker-owned, no allocation: 64 fixed slots; a transaction that finds the table full applies at its own offsets (FoldWalk.h).
#pragma once
#include <array>
#include <cstdint>

namespace rune::view {

struct TxnSpan {
    int64_t pid   = -1;
    int64_t first = -1;   // the offset of the transaction's first batch this fold met
};

struct TxnSpanFence {
    static constexpr std::size_t kSlots = 64;

    std::array<TxnSpan, kSlots> open{};
    uint8_t count = 0;
    // A marker application the pass budget stopped: the span's records at or below it are applied, the fold stands at the marker.
    // Only the walk that finishes that application resets it: closing another span (an abort marker, the judge) leaves it.
    int64_t applyingThrough = -1;
    // An atomic batch the pass cap cut ends here: its records at or below the cursor are applied, the rest are not yet.
    int64_t atomicThrough = -1;
    // The worker's supersede key set: the token this fence holds it by (0 = none), and the holder's use count a wait last saw.
    uint64_t noteToken   = 0;
    uint64_t seenTouches = UINT64_MAX;

    /// False when every slot is taken.
    [[nodiscard]] bool add(int64_t pid, int64_t first) noexcept {
        if (count == kSlots) return false;
        open[count++] = TxnSpan{pid, first};
        return true;
    }
    [[nodiscard]] const TxnSpan* find(int64_t pid) const noexcept {
        for (uint8_t i = 0; i < count; ++i)
            if (open[i].pid == pid) return &open[i];
        return nullptr;
    }
    void close(int64_t pid) noexcept {
        for (uint8_t i = 0; i < count; ++i) {
            if (open[i].pid != pid) continue;
            open[i] = open[--count];
            open[count] = TxnSpan{};
            return;
        }
    }
    /// A marker or atomic batch application is in progress: no read may see the instance's tables.
    [[nodiscard]] bool fenced() const noexcept { return applyingThrough >= 0 || atomicThrough >= 0; }
    /// Some open span starts below `o`: a checkpoint at `o` would hold none of its records and a replay from `o` would never see them.
    [[nodiscard]] bool straddles(int64_t o) const noexcept {
        for (uint8_t i = 0; i < count; ++i)
            if (open[i].first < o) return true;
        return false;
    }
    /// The fold cursor was reset (a discard, a rebuild from the log start): nothing is open any more.
    void clear() noexcept {
        for (uint8_t i = 0; i < count; ++i) open[i] = TxnSpan{};
        count = 0;
        applyingThrough = atomicThrough = -1;
        noteToken = 0;
        seenTouches = UINT64_MAX;
    }
};

} // namespace rune::view
