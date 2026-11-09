#pragma once
// LeaseWheel.h - lease topics Task 9 (spec 9.1, 9.5): a hierarchical hashed timer wheel over LeaseEntryStore entries
// (5 levels x 1024 slots, base tick 1 ms, level l tick = 1024^l ms), intrusive doubly linked lists by u32 row id.
// Fixed size, no allocation, no virtual call.  Owned by one thread (shared-nothing).
//
// Callers compute every deadline with satAdd (LeaseEntryStore.h).  A saturated INT64_MAX deadline lands in the top level.
// The wheel OWNS the entry's deadlineNs (Deadline) / checkpointNs (Checkpoint) while the entry is linked: it stores the value in
// arm() and derives the slot from it in unlink(), so change it only through arm() (LeaseTimers::rearm does).  LeaseTimers is the
// only way the rest of the node reaches the entries (its members are private).
#include <rune/node/lease/LeaseEntryStore.h>

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstdint>

namespace rune::node::lease {

enum class LeaseWheelKind : uint8_t { Deadline, Checkpoint };

inline constexpr int      kLeaseWheelLevels = 5;
inline constexpr int      kLeaseSlotBits    = 10;
inline constexpr uint32_t kLeaseWheelSlots  = 1u << kLeaseSlotBits;
inline constexpr uint32_t kLeaseSlotMask    = kLeaseWheelSlots - 1;
inline constexpr uint8_t  kLeaseReadyLevel  = kLeaseWheelLevels;   // level value of the ready list
inline constexpr int64_t  kLeaseWheelTickNs = 1'000'000;           // one tick = 1 ms
/// poll examines at most kLeaseExamineFactor * max ready rows (taken + deferred), so a deferring sink stays O(max).
inline constexpr uint32_t kLeaseExamineFactor = 2;

template <LeaseWheelKind K>
class LeaseWheel {
public:
    /// The cursor starts at `nowNs` (a wheel created at a large clock value does not walk up from tick 0).
    LeaseWheel(LeaseEntryStore& store, int64_t nowNs) noexcept : store_(&store) { reset(nowNs); }

    /// Unlinks nothing (the caller clears the store first).
    void reset(int64_t nowNs) noexcept {
        for (auto& lvl : head_) for (auto& h : lvl) h = kLeaseNil;
        for (auto& lvl : occ_) for (auto& w : lvl) w = 0;
        ready_       = kLeaseNil;
        readyTail_   = kLeaseNil;
        cur_         = nowTick(nowNs);
        nextCheckNs_ = INT64_MAX;
        lastPollNs_  = INT64_MIN;
    }

    /// Stores `deadlineNs` in the entry and (re)links it.
    void arm(uint32_t rid, int64_t deadlineNs) noexcept {
        unlink(rid);
        whenOf(store_->at(rid)) = deadlineNs;
        place(rid, tickOf(deadlineNs));
    }

    /// Leaves nextCheckNs_ alone: a stale early value only causes one empty visit.
    void cancel(uint32_t rid) noexcept { unlink(rid); }

    [[nodiscard]] bool linked(uint32_t rid) const noexcept { return levelOfEntry(store_->at(rid)) != kLeaseUnlinked; }
    [[nodiscard]] uint8_t levelOf(uint32_t rid) const noexcept { return levelOfEntry(store_->at(rid)); }
    [[nodiscard]] int64_t nextCheckNs() const noexcept { return nextCheckNs_; }
    [[nodiscard]] int64_t curNs() const noexcept { return tickToNs(cur_); }
    [[nodiscard]] uint64_t touchedForTest() const noexcept { return touched_; }
    [[nodiscard]] uint64_t slotsVisitedForTest() const noexcept { return slotsVisited_; }
    [[nodiscard]] uint64_t invariantRepairsForTest() const noexcept { return invariantRepairs_; }

    /// Hands due rows to `sink(rid) -> bool`, at most `max` taken.  The row is unlinked before `sink(rid)` is called.  The sink may
    /// re-arm it, cancel it or change its state (through LeaseTimers).  Return true = taken (counts toward `max`); false =
    /// deferred: leave the row untouched and it goes back to the ready list's tail (the ready list is FIFO).  The sink may change
    /// ONLY the row it was given; a sink that cancels, re-arms or changes the state of its row must return true.  A deferred row is not examined again in the same call, and at most kLeaseExamineFactor * max
    /// ready rows are examined per call (a deferring sink costs O(max), not O(ready)).
    /// The clock is monotonic by contract (asserted): `nowNs` never goes back between calls.
    /// Due set, exactly: the rows armed with dTick <= nowTick; never early (every handed row has deadlineNs <= nowNs).
    /// Cost: O(due + slots taken + 5 x 16 words per visited tick); each entry cascades at most 4 times.
    template <class Sink>
    uint32_t poll(int64_t nowNs, Sink&& sink, uint32_t max) {
        assert(nowNs >= lastPollNs_ && "the lease clock is monotonic");
        lastPollNs_ = nowNs;
        if (nowNs < nextCheckNs_) return 0;   // the idle path is this one compare

        const int64_t target = nowTick(nowNs);
        for (;;) {
            const int64_t t = nextEventTick();
            if (t > target) break;
            if (t < cur_) [[unlikely]] {   // a level >= 1 slot of the cursor's own block: only a broken invariant
                ++invariantRepairs_;
                assert(false && "LeaseWheel: occupied slot behind the cursor");
                repairBehind();
                continue;
            }
            cur_ = t;
            for (int l = kLeaseWheelLevels - 1; l >= 1; --l) {
                const int64_t mask = (int64_t{1} << (kLeaseSlotBits * l)) - 1;
                if ((t & mask) != 0) continue;
                const uint32_t s = static_cast<uint32_t>((t >> (kLeaseSlotBits * l)) & kLeaseSlotMask);
                if (!occupied(l, s)) continue;
                ++slotsVisited_;
                uint32_t rid = head_[l][s];
                head_[l][s]  = kLeaseNil;
                clearOcc(l, s);
                while (rid != kLeaseNil) {
                    LeaseEntry& e   = store_->at(rid);
                    const uint32_t n = nextOf(e);
                    levelRef(e)      = kLeaseUnlinked;
                    ++touched_;
                    place(rid, tickOf(whenOf(e)));
                    rid = n;
                }
            }
            const uint32_t s0 = static_cast<uint32_t>(t & kLeaseSlotMask);
            if (occupied(0, s0)) {
                ++slotsVisited_;
                uint32_t rid = head_[0][s0];
                head_[0][s0] = kLeaseNil;
                clearOcc(0, s0);
                while (rid != kLeaseNil) {
                    LeaseEntry& e   = store_->at(rid);
                    const uint32_t n = nextOf(e);
                    ++touched_;
                    pushReady(rid);
                    rid = n;
                }
            }
            cur_ = t + 1;
        }
        cur_ = std::max(cur_, target + 1);

        uint32_t count        = 0;
        uint64_t examined     = 0;
        uint32_t firstDeferred = kLeaseNil;
        const uint64_t examineCap = static_cast<uint64_t>(kLeaseExamineFactor) * max;
        uint32_t rid = ready_;
        while (rid != kLeaseNil && rid != firstDeferred && count < max && examined < examineCap) {
            ++examined;
            const uint32_t n = nextOf(store_->at(rid));
            ++touched_;
            unlink(rid);   // the sink owns the row from here on
            if (sink(rid)) {
                ++count;
            } else {
                // a deferring sink leaves the row untouched: still Armed and unlinked (a sink that cancels, re-arms or changes
                // the state must return true)
                assert(levelOfEntry(store_->at(rid)) == kLeaseUnlinked && "deferred row was relinked by the sink");
                if (levelOfEntry(store_->at(rid)) == kLeaseUnlinked) pushReady(rid);   // deferred: back at the FIFO tail
            }
            // a row now at the ready list's tail (deferred, or re-armed into the past by the sink) is not walked again
            if (firstDeferred == kLeaseNil && levelOfEntry(store_->at(rid)) == kLeaseReadyLevel) firstDeferred = rid;
            rid = n;
        }

        if (ready_ != kLeaseNil) {
            nextCheckNs_ = INT64_MIN;
        } else {
            const int64_t t = nextEventTick();
            nextCheckNs_    = t == kNoTick ? INT64_MAX : tickToNs(t);
        }
        return count;
    }

private:
    static constexpr int64_t kNoTick = INT64_MAX;
    static constexpr uint32_t kWords = kLeaseWheelSlots / 64;

    static uint32_t& prevOf(LeaseEntry& e) noexcept {
        if constexpr (K == LeaseWheelKind::Deadline) return e.dPrev; else return e.cPrev;
    }
    static uint32_t& nextOf(LeaseEntry& e) noexcept {
        if constexpr (K == LeaseWheelKind::Deadline) return e.dNext; else return e.cNext;
    }
    static uint8_t& levelRef(LeaseEntry& e) noexcept {
        if constexpr (K == LeaseWheelKind::Deadline) return e.levelD; else return e.levelC;
    }
    static int64_t& whenOf(LeaseEntry& e) noexcept {
        if constexpr (K == LeaseWheelKind::Deadline) return e.deadlineNs; else return e.checkpointNs;
    }
    static uint8_t levelOfEntry(const LeaseEntry& e) noexcept {
        if constexpr (K == LeaseWheelKind::Deadline) return e.levelD; else return e.levelC;
    }

    /// ceil(ns / 1e6) without overflow; a negative deadline is tick 0.
    static int64_t tickOf(int64_t ns) noexcept {
        if (ns <= 0) return 0;
        return ns / kLeaseWheelTickNs + (ns % kLeaseWheelTickNs != 0 ? 1 : 0);
    }
    static int64_t nowTick(int64_t ns) noexcept { return ns <= 0 ? 0 : ns / kLeaseWheelTickNs; }
    static int64_t tickToNs(int64_t tick) noexcept {
        return tick > INT64_MAX / kLeaseWheelTickNs ? INT64_MAX : tick * kLeaseWheelTickNs;
    }

    [[nodiscard]] bool occupied(int l, uint32_t s) const noexcept { return (occ_[l][s >> 6] >> (s & 63)) & 1u; }
    void setOcc(int l, uint32_t s) noexcept { occ_[l][s >> 6] |= uint64_t{1} << (s & 63); }
    void clearOcc(int l, uint32_t s) noexcept { occ_[l][s >> 6] &= ~(uint64_t{1} << (s & 63)); }

    /// Appends at the ready list's tail (FIFO).
    void pushReady(uint32_t rid) noexcept {
        LeaseEntry& e = store_->at(rid);
        prevOf(e)     = readyTail_;
        nextOf(e)     = kLeaseNil;
        levelRef(e)   = kLeaseReadyLevel;
        if (readyTail_ != kLeaseNil) nextOf(store_->at(readyTail_)) = rid;
        else ready_ = rid;
        readyTail_ = rid;
    }

    /// The entry must be unlinked.  Invariant: for l >= 1 the chosen block is strictly after cur_'s block (the lowest level l
    /// with a 1024-block diff < 1024 means level l-1 had diff >= 1024), so its cascade tick (the block start) is still ahead.
    void place(uint32_t rid, int64_t dTick) noexcept {
        if (dTick < cur_) {
            pushReady(rid);
            nextCheckNs_ = INT64_MIN;
            return;
        }
        int l = 0;
        while (l < kLeaseWheelLevels - 1 &&
               ((dTick >> (kLeaseSlotBits * l)) - (cur_ >> (kLeaseSlotBits * l))) >= static_cast<int64_t>(kLeaseWheelSlots)) ++l;
        const uint32_t s = static_cast<uint32_t>((dTick >> (kLeaseSlotBits * l)) & kLeaseSlotMask);
        LeaseEntry& e    = store_->at(rid);
        prevOf(e)        = kLeaseNil;
        nextOf(e)        = head_[l][s];
        levelRef(e)      = static_cast<uint8_t>(l);
        if (head_[l][s] != kLeaseNil) prevOf(store_->at(head_[l][s])) = rid;
        head_[l][s] = rid;
        setOcc(l, s);
        const int64_t eventTick = l == 0 ? dTick : (dTick >> (kLeaseSlotBits * l)) << (kLeaseSlotBits * l);
        nextCheckNs_            = std::min(nextCheckNs_, tickToNs(eventTick));
    }

    void unlink(uint32_t rid) noexcept {
        LeaseEntry& e     = store_->at(rid);
        const uint8_t lvl = levelRef(e);
        if (lvl == kLeaseUnlinked) return;
        const uint32_t p = prevOf(e);
        const uint32_t n = nextOf(e);
        if (lvl == kLeaseReadyLevel) {
            assert(p != kLeaseNil || ready_ == rid);        // no prev: the head of the ready list
            assert(n != kLeaseNil || readyTail_ == rid);    // no next: the tail of the ready list
            if (p != kLeaseNil) nextOf(store_->at(p)) = n; else ready_ = n;
            if (n != kLeaseNil) prevOf(store_->at(n)) = p; else readyTail_ = p;
        } else {
            if (p != kLeaseNil) {
                nextOf(store_->at(p)) = n;
            } else {
                const uint32_t s = static_cast<uint32_t>((tickOf(whenOf(e)) >> (kLeaseSlotBits * lvl)) & kLeaseSlotMask);
                assert(head_[lvl][s] == rid);   // no prev: the head of the slot its stored time names
                head_[lvl][s]    = n;
                if (n == kLeaseNil) clearOcc(lvl, s);
            }
            if (n != kLeaseNil) prevOf(store_->at(n)) = p;
        }
        prevOf(e)   = kLeaseNil;
        nextOf(e)   = kLeaseNil;
        levelRef(e) = kLeaseUnlinked;
    }

    /// Re-places every row of each level's cursor-block slot (l >= 1) at the current cursor.
    void repairBehind() noexcept {
        for (int l = kLeaseWheelLevels - 1; l >= 1; --l) {
            const uint32_t s = static_cast<uint32_t>((cur_ >> (kLeaseSlotBits * l)) & kLeaseSlotMask);
            if (!occupied(l, s)) continue;
            uint32_t rid = head_[l][s];
            head_[l][s]  = kLeaseNil;
            clearOcc(l, s);
            while (rid != kLeaseNil) {
                LeaseEntry& e   = store_->at(rid);
                const uint32_t n = nextOf(e);
                levelRef(e)      = kLeaseUnlinked;
                place(rid, tickOf(whenOf(e)));
                rid = n;
            }
        }
    }

    /// First occupied slot of level l at or after `pos`, wrapping once around the slots; -1 if none.
    [[nodiscard]] int firstSlot(int l, uint32_t pos) const noexcept {
        const uint32_t w = pos >> 6, b = pos & 63;
        for (uint32_t i = 0; i <= kWords; ++i) {
            const uint32_t idx = (w + i) & (kWords - 1);
            uint64_t bits      = occ_[l][idx];
            if (i == 0) bits &= ~uint64_t{0} << b;
            else if (i == kWords) bits &= (uint64_t{1} << b) - 1;
            if (bits != 0) return static_cast<int>(idx * 64 + static_cast<uint32_t>(std::countr_zero(bits)));
        }
        return -1;
    }

    /// The minimum over levels of the tick of the first occupied slot at or after cur_'s position (block start for l >= 1).
    [[nodiscard]] int64_t nextEventTick() const noexcept {
        int64_t best = kNoTick;
        for (int l = 0; l < kLeaseWheelLevels; ++l) {
            const int64_t base = cur_ >> (kLeaseSlotBits * l);
            const uint32_t pos = static_cast<uint32_t>(base & kLeaseSlotMask);
            const int s        = firstSlot(l, pos);
            if (s < 0) continue;
            const int64_t k = (static_cast<int64_t>(s) - pos) & kLeaseSlotMask;
            best            = std::min(best, (base + k) << (kLeaseSlotBits * l));
        }
        return best;
    }

    LeaseEntryStore* store_;
    uint32_t head_[kLeaseWheelLevels][kLeaseWheelSlots];
    uint64_t occ_[kLeaseWheelLevels][kWords];
    uint32_t ready_       = kLeaseNil;   // FIFO: head ...
    uint32_t readyTail_   = kLeaseNil;   // ... and tail
    int64_t  cur_         = 0;           // the next tick to process: every tick < cur_ is processed
    int64_t  nextCheckNs_ = INT64_MAX;
    int64_t  lastPollNs_  = INT64_MIN;
    uint64_t touched_      = 0;
    uint64_t slotsVisited_ = 0;
    uint64_t invariantRepairs_ = 0;
};

/// The store plus both wheels, and the ONE place that moves a linked row: "only Armed is linked" (spec 9.1, R4).  Not movable
/// (the wheels point at the store).  The store and the wheels are private: the rest of the node reads entries through entry() and
/// the const wheel accessors, and changes them only through the methods below.
class LeaseTimers {
public:
    /// Both wheel cursors start at `nowNs`.
    LeaseTimers(uint32_t maxKeys, int64_t nowNs) : store_(maxKeys), deadline_(store_, nowNs), checkpoint_(store_, nowNs) {}
    LeaseTimers(const LeaseTimers&)            = delete;
    LeaseTimers& operator=(const LeaseTimers&) = delete;

    [[nodiscard]] const LeaseEntry& entry(uint32_t rid) const noexcept { return store_.at(rid); }
    [[nodiscard]] bool covers(uint32_t rid) const noexcept { return store_.covers(rid); }
    /// Cold.  A grant fold calls it before `arm` whenever `!covers(rid)`: the 75 % rule leaves only 1024 rids of headroom at 4096,
    /// and one fold can add more grants than that.  False (nothing added) past the cap.
    [[nodiscard]] bool reserveFor(uint32_t rid) { return store_.reserveFor(rid); }
    void noteUse(uint32_t rid) noexcept { store_.noteUse(rid); }
    [[nodiscard]] bool growOwed() const noexcept { return store_.growOwed(); }
    void growIfOwed() { store_.growIfOwed(); }   // cold
    [[nodiscard]] uint32_t capacity() const noexcept { return store_.capacity(); }
    [[nodiscard]] std::size_t bytes() const noexcept { return store_.bytes(); }

    /// The entry must be Free (a fresh or reset row).  Stores both times and enters Armed: links the deadline wheel and, if
    /// checkpointNs != kLeaseNoCheckpoint, the checkpoint wheel.
    void arm(uint32_t rid, int64_t deadlineNs, int64_t checkpointNs) noexcept {
        LeaseEntry& e = store_.at(rid);
        assert(e.state == LeaseEntryState::Free);
        e.state          = LeaseEntryState::Armed;
        e.inflightOffset = -1;
        e.checkpointNs   = checkpointNs;
        deadline_.arm(rid, deadlineNs);
        if (checkpointNs != kLeaseNoCheckpoint) checkpoint_.arm(rid, checkpointNs);
    }

    /// Leaving Armed unlinks both wheels (any state other than Armed is never linked); entering Armed (from any other state)
    /// links the deadline wheel at deadlineNs and, if checkpointNs != kLeaseNoCheckpoint, the checkpoint wheel; entering Free
    /// resets the entry to LeaseEntry{}.
    void setState(uint32_t rid, LeaseEntryState s, int64_t inflightOffset = -1) noexcept {
        LeaseEntry& e = store_.at(rid);
        if (s != LeaseEntryState::Armed) {   // cancel is a no-op on an unlinked row
            deadline_.cancel(rid);
            checkpoint_.cancel(rid);
        }
        if (s == LeaseEntryState::Free) {
            e = LeaseEntry{};
            return;
        }
        const bool entering = e.state != LeaseEntryState::Armed && s == LeaseEntryState::Armed;
        e.state             = s;
        e.inflightOffset    = s == LeaseEntryState::Armed ? -1 : inflightOffset;
        if (entering) {
            deadline_.arm(rid, e.deadlineNs);
            if (e.checkpointNs != kLeaseNoCheckpoint) checkpoint_.arm(rid, e.checkpointNs);
        }
    }

    /// Store the deadline; link only if the entry is Armed.
    void rearm(uint32_t rid, int64_t deadlineNs) noexcept {
        LeaseEntry& e = store_.at(rid);
        if (e.state == LeaseEntryState::Armed) deadline_.arm(rid, deadlineNs);
        else e.deadlineNs = deadlineNs;
    }

    /// Store the checkpoint time; link only if the entry is Armed (kLeaseNoCheckpoint cancels the schedule).
    void scheduleCheckpoint(uint32_t rid, int64_t checkpointNs) noexcept {
        LeaseEntry& e = store_.at(rid);
        if (e.state == LeaseEntryState::Armed) {
            if (checkpointNs == kLeaseNoCheckpoint) {
                checkpoint_.cancel(rid);
                e.checkpointNs = checkpointNs;
            } else {
                checkpoint_.arm(rid, checkpointNs);
            }
        } else {
            e.checkpointNs = checkpointNs;
        }
    }

    /// The entry is Armed and unlinked from the deadline wheel (a poll handed it and the caller could not use it, e.g. an append
    /// was refused): re-places it at its stored deadline.  A row a sink took stays unlinked until relink() or rearm(); note
    /// setState(Armed) on an already-Armed row does NOT relink it.  There is no checkpoint-wheel relink: a checkpoint sink that
    /// cannot use its row returns false (deferral) or reschedules it with scheduleCheckpoint().
    void relink(uint32_t rid) noexcept {
        const LeaseEntry& e = store_.at(rid);
        assert(e.state == LeaseEntryState::Armed);
        assert(!deadline_.linked(rid));
        deadline_.arm(rid, e.deadlineNs);
    }

    /// See LeaseWheel::poll for the sink contract.
    /// A deferring sink (returns false) must leave its row Armed.
    template <class Sink>
    uint32_t pollDeadline(int64_t nowNs, Sink&& sink, uint32_t max) {
        return deadline_.poll(nowNs, [&](uint32_t rid) { return checkedSink(sink, rid); }, max);
    }
    template <class Sink>
    uint32_t pollCheckpoint(int64_t nowNs, Sink&& sink, uint32_t max) {
        return checkpoint_.poll(nowNs, [&](uint32_t rid) { return checkedSink(sink, rid); }, max);
    }

    [[nodiscard]] const LeaseWheel<LeaseWheelKind::Deadline>& deadline() const noexcept { return deadline_; }
    [[nodiscard]] const LeaseWheel<LeaseWheelKind::Checkpoint>& checkpoint() const noexcept { return checkpoint_; }

private:
    template <class Sink>
    bool checkedSink(Sink& sink, uint32_t rid) {
        const bool taken = sink(rid);
        assert((taken || store_.at(rid).state == LeaseEntryState::Armed) && "a deferred row changed state");
        return taken;
    }
    LeaseEntryStore store_;
    LeaseWheel<LeaseWheelKind::Deadline> deadline_;
    LeaseWheel<LeaseWheelKind::Checkpoint> checkpoint_;
};

}  // namespace rune::node::lease
