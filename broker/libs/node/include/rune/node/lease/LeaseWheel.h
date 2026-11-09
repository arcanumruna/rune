#pragma once
// LeaseWheel.h - lease topics Task 9 (spec 9.1, 9.5): a hierarchical hashed timer wheel over LeaseEntryStore entries
// (5 levels x 1024 slots, base tick 1 ms, level l tick = 1024^l ms), intrusive doubly linked lists by u32 row id.
// Fixed size, no allocation, no virtual call.  Owned by one thread (shared-nothing).
//
// Callers compute every deadline with satAdd (LeaseEntryStore.h).  A saturated INT64_MAX deadline lands in level 4.
// The wheel OWNS the entry's deadlineNs (Deadline) / checkpointNs (Checkpoint) while the entry is linked: it stores the value in
// arm() and derives the slot from it in unlink(), so change it only through arm() (LeaseTimers::rearm does).
#include <rune/node/lease/LeaseEntryStore.h>

#include <algorithm>
#include <bit>
#include <cstdint>

namespace rune::node::lease {

enum class LeaseWheelKind : uint8_t { Deadline, Checkpoint };

inline constexpr uint8_t  kLeaseReadyLevel = 5;          // level value of the ready list
inline constexpr int64_t  kLeaseTickNs     = 1'000'000;  // one tick = 1 ms

template <LeaseWheelKind K>
class LeaseWheel {
public:
    explicit LeaseWheel(LeaseEntryStore& store) noexcept : store_(&store) { reset(0); }

    /// Unlinks nothing (the caller clears the store first).
    void reset(int64_t nowNs) noexcept {
        for (auto& lvl : head_) for (auto& h : lvl) h = kLeaseNil;
        for (auto& lvl : occ_) for (auto& w : lvl) w = 0;
        ready_       = kLeaseNil;
        cur_         = nowTick(nowNs);
        nextCheckNs_ = INT64_MAX;
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

    /// Hands due rows to `sink(rid) -> bool`, at most `max` accepted.  A false return leaves the row linked in the ready list (due,
    /// deferred).  The sink may change ONLY the row it was given (it may unlink it; the wheel re-checks).
    /// Due set, exactly: the rows armed with dTick <= nowTick; never early (every handed row has deadlineNs <= nowNs).
    /// Cost: O(due + slots taken + 5 x 16 words per visited tick); each entry cascades at most 4 times.
    template <class Sink>
    uint32_t poll(int64_t nowNs, Sink&& sink, uint32_t max) {
        if (nowNs < nextCheckNs_) return 0;   // the idle path is this one compare

        const int64_t target = nowTick(nowNs);
        for (;;) {
            const int64_t t = nextEventTick();
            if (t > target) break;
            cur_ = t;
            for (int l = 4; l >= 1; --l) {
                const int64_t mask = (int64_t{1} << (10 * l)) - 1;
                if ((t & mask) != 0) continue;
                const uint32_t s = static_cast<uint32_t>((t >> (10 * l)) & 1023);
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
            const uint32_t s0 = static_cast<uint32_t>(t & 1023);
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

        uint32_t count = 0;
        uint32_t rid   = ready_;
        while (rid != kLeaseNil && count < max) {
            const uint32_t n = nextOf(store_->at(rid));
            ++touched_;
            if (sink(rid)) {
                unlink(rid);   // no-op if the sink already changed the row
                ++count;
            }
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
        return ns / kLeaseTickNs + (ns % kLeaseTickNs != 0 ? 1 : 0);
    }
    static int64_t nowTick(int64_t ns) noexcept { return ns <= 0 ? 0 : ns / kLeaseTickNs; }
    static int64_t tickToNs(int64_t tick) noexcept {
        return tick > INT64_MAX / kLeaseTickNs ? INT64_MAX : tick * kLeaseTickNs;
    }

    [[nodiscard]] bool occupied(int l, uint32_t s) const noexcept { return (occ_[l][s >> 6] >> (s & 63)) & 1u; }
    void setOcc(int l, uint32_t s) noexcept { occ_[l][s >> 6] |= uint64_t{1} << (s & 63); }
    void clearOcc(int l, uint32_t s) noexcept { occ_[l][s >> 6] &= ~(uint64_t{1} << (s & 63)); }

    void pushReady(uint32_t rid) noexcept {
        LeaseEntry& e = store_->at(rid);
        prevOf(e)     = kLeaseNil;
        nextOf(e)     = ready_;
        levelRef(e)   = kLeaseReadyLevel;
        if (ready_ != kLeaseNil) prevOf(store_->at(ready_)) = rid;
        ready_ = rid;
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
        while (l < 4 && ((dTick >> (10 * l)) - (cur_ >> (10 * l))) >= 1024) ++l;
        const uint32_t s = static_cast<uint32_t>((dTick >> (10 * l)) & 1023);
        LeaseEntry& e    = store_->at(rid);
        prevOf(e)        = kLeaseNil;
        nextOf(e)        = head_[l][s];
        levelRef(e)      = static_cast<uint8_t>(l);
        if (head_[l][s] != kLeaseNil) prevOf(store_->at(head_[l][s])) = rid;
        head_[l][s] = rid;
        setOcc(l, s);
        const int64_t eventTick = l == 0 ? dTick : (dTick >> (10 * l)) << (10 * l);
        nextCheckNs_            = std::min(nextCheckNs_, tickToNs(eventTick));
    }

    void unlink(uint32_t rid) noexcept {
        LeaseEntry& e     = store_->at(rid);
        const uint8_t lvl = levelRef(e);
        if (lvl == kLeaseUnlinked) return;
        const uint32_t p = prevOf(e);
        const uint32_t n = nextOf(e);
        if (p != kLeaseNil) {
            nextOf(store_->at(p)) = n;
        } else if (lvl == kLeaseReadyLevel) {
            ready_ = n;
        } else {
            const uint32_t s = static_cast<uint32_t>((tickOf(whenOf(e)) >> (10 * lvl)) & 1023);
            head_[lvl][s]    = n;
            if (n == kLeaseNil) clearOcc(lvl, s);
        }
        if (n != kLeaseNil) prevOf(store_->at(n)) = p;
        prevOf(e)   = kLeaseNil;
        nextOf(e)   = kLeaseNil;
        levelRef(e) = kLeaseUnlinked;
    }

    /// First occupied slot of level l at or after `pos`, wrapping once around the 1024 slots; -1 if none.
    [[nodiscard]] int firstSlot(int l, uint32_t pos) const noexcept {
        const uint32_t w = pos >> 6, b = pos & 63;
        for (uint32_t i = 0; i <= 16; ++i) {
            const uint32_t idx = (w + i) & 15;
            uint64_t bits      = occ_[l][idx];
            if (i == 0) bits &= ~uint64_t{0} << b;
            else if (i == 16) bits &= (uint64_t{1} << b) - 1;
            if (bits != 0) return static_cast<int>(idx * 64 + static_cast<uint32_t>(std::countr_zero(bits)));
        }
        return -1;
    }

    /// The minimum over levels of the tick of the first occupied slot at or after cur_'s position (block start for l >= 1).
    [[nodiscard]] int64_t nextEventTick() const noexcept {
        int64_t best = kNoTick;
        for (int l = 0; l < 5; ++l) {
            const int64_t base = cur_ >> (10 * l);
            const uint32_t pos = static_cast<uint32_t>(base & 1023);
            const int s        = firstSlot(l, pos);
            if (s < 0) continue;
            const int64_t k = (static_cast<int64_t>(s) - pos) & 1023;
            best            = std::min(best, (base + k) << (10 * l));
        }
        return best;
    }

    LeaseEntryStore* store_;
    uint32_t head_[5][1024];
    uint64_t occ_[5][16];
    uint32_t ready_       = kLeaseNil;
    int64_t  cur_         = 0;   // the next tick to process: every tick < cur_ is processed
    int64_t  nextCheckNs_ = INT64_MAX;
    uint64_t touched_      = 0;
    uint64_t slotsVisited_ = 0;
};

/// The store plus both wheels, and the ONE place that keeps "only Armed is linked" (spec 9.1, R4).  Not movable (the wheels
/// point at the store).
struct LeaseTimers {
    LeaseEntryStore store;
    LeaseWheel<LeaseWheelKind::Deadline> deadline;
    LeaseWheel<LeaseWheelKind::Checkpoint> checkpoint;

    explicit LeaseTimers(uint32_t maxKeys) : store(maxKeys), deadline(store), checkpoint(store) {}
    LeaseTimers(const LeaseTimers&)            = delete;
    LeaseTimers& operator=(const LeaseTimers&) = delete;

    /// Leaving Armed unlinks both wheels; entering Armed (from any other state) links the deadline wheel at deadlineNs and, if
    /// checkpointNs != kLeaseNoCheckpoint, the checkpoint wheel; entering Free resets the entry to LeaseEntry{}.
    void setState(uint32_t rid, LeaseEntryState s, int64_t inflightOffset = -1) noexcept {
        LeaseEntry& e = store.at(rid);
        if (e.state == LeaseEntryState::Armed && s != LeaseEntryState::Armed) {
            deadline.cancel(rid);
            checkpoint.cancel(rid);
        }
        if (s == LeaseEntryState::Free) {
            e = LeaseEntry{};
            return;
        }
        const bool entering = e.state != LeaseEntryState::Armed && s == LeaseEntryState::Armed;
        e.state             = s;
        e.inflightOffset    = s == LeaseEntryState::Armed ? -1 : inflightOffset;
        if (entering) {
            deadline.arm(rid, e.deadlineNs);
            if (e.checkpointNs != kLeaseNoCheckpoint) checkpoint.arm(rid, e.checkpointNs);
        }
    }

    /// Store the deadline; link only if the entry is Armed.
    void rearm(uint32_t rid, int64_t deadlineNs) noexcept {
        LeaseEntry& e = store.at(rid);
        if (e.state == LeaseEntryState::Armed) deadline.arm(rid, deadlineNs);
        else e.deadlineNs = deadlineNs;
    }

    /// Store the checkpoint time; link only if the entry is Armed (kLeaseNoCheckpoint cancels the schedule).
    void scheduleCheckpoint(uint32_t rid, int64_t checkpointNs) noexcept {
        LeaseEntry& e = store.at(rid);
        if (e.state == LeaseEntryState::Armed) {
            if (checkpointNs == kLeaseNoCheckpoint) {
                checkpoint.cancel(rid);
                e.checkpointNs = checkpointNs;
            } else {
                checkpoint.arm(rid, checkpointNs);
            }
        } else {
            e.checkpointNs = checkpointNs;
        }
    }

    /// Demotion: every entry Free, both wheels empty (cursor kept).
    void demote() noexcept {
        store.clear();
        deadline.reset(deadline.curNs());
        checkpoint.reset(checkpoint.curNs());
    }
};

}  // namespace rune::node::lease
