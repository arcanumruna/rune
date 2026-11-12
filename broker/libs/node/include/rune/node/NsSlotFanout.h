#pragma once
// NsSlotFanout.h - CONTROL-thread fan-out of namespace slot messages to EVERY worker's ingress ring (Task 8, D22).
// The control thread owns all of it; each worker owns its slot table.
//
// Two kinds of message, two bounded stores (#710 / #729: nothing here grows or allocates after configure()):
//
//   ORDERED  (NsSlotAssign, NsSlotRelease, the NsDeleting throttle): a fixed-capacity FIFO.  Each message goes to the
//            workers in index order; a full ring is retried next duty cycle and the FIFO stalls behind it, so per
//            worker the messages stay in FIFO order (the same ring as TopicDropped and every produce op, so an assign
//            always precedes the first op of a topic that carries its slot).
//            An entry that no worker has seen yet (nextWorker == 0) is EDITED IN PLACE instead of appended to
//            (#1020): a limits change of an owner whose Assign is still unseen overwrites that Assign; a Release of
//            an owner whose fresh Assign (and NsDeleting) is still unseen cancels both (they become dead entries that
//            pump skips, and no Release is queued); a dead Assign / NsDeleting entry is revived by the next owner of
//            the slot.  Only the per-slot order matters to a worker, and an unseen entry can be changed freely.  At
//            most ONE queued entry is partly delivered (a full ring stalls the FIFO at the first undelivered entry),
//            every entry behind it is unseen.  Every entry has an absolute sequence number (headSeq_ is the one of
//            the head); per slot the newest queued Assign and NsDeleting entries are remembered by sequence.
//            A Release cancels only a FRESH Assign (the owner was announced by no earlier Assign); the Assign of a
//            limits change of an already announced owner is a refresh and is never cancelled (the workers hold it).
//            Bound: per slot the queue holds at most {the Assign of the already delivered owner's new limits (a
//            refresh, merged in place), its NsDeleting, its Release, one Assign entry and one NsDeleting entry for
//            the next owner (live or dead, reused by every later owner)} = 5 entries, plus the one partly delivered
//            entry and a Release behind it.  Capacity = max(16, 6 * slotCapacity + 16).  A repeated NsDeleting of one
//            owner is dropped (the worker's gate is sticky until the release).  A post on a full FIFO is therefore
//            unreachable; it is still counted and logged at CRITICAL, never silent.
//   LATEST-WINS (the rate / disk Throttle kinds, ViewMemOver and LeasesOver, posted at the aggregator's tick rate): a fixed table
//            latest_[slot * kKinds + kind] plus, per worker, a dirty list.  A post overwrites the slot's value and marks
//            it dirty for every worker; an offer delivers the LAST value once.  With a full ring the table stays dirty,
//            so sustained throttling costs no memory.
//
// Ordering: a worker's dirty entries are offered only when it has no ordered message pending, so an assign is always
// delivered before any throttle of that slot.  A release voids the slot's owed cells here, so a throttle of the old
// owner is never delivered to the new one; the worker's slot tag check (`nsSlots_.matches`) stays as the second line
// of defence.
//
// pump() is a template over the pool so a test can drive it with a stub whose rings refuse; the production type is
// WorkerPool.  No virtual call, no std::function.

#include <rune/node/NsRingMessages.h>
#include <rune/node/WorkerPool.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <variant>
#include <vector>

namespace rune::node {

class NsSlotFanout {
public:
    /// The rate / disk throttle kinds plus ViewMemOver and LeasesOver: the latest-wins table has one column for each.
    static constexpr std::size_t kKinds = kThrottleKinds + 2;
    static constexpr std::size_t kViewMemOverKind = kThrottleKinds;
    static constexpr std::size_t kLeasesOverKind  = kThrottleKinds + 1;   // Lease topics Task 21

    /// Cold, startup, before the first post: sizes every store.  Calling it again resets them (nothing pending).
    void configure(uint32_t slotCapacity, int32_t workerCount) {
        slotCapacity_ = slotCapacity;
        workers_      = workerCount < 1 ? 1 : static_cast<uint32_t>(workerCount);
        const std::size_t fifoCap = std::max<std::size_t>(16, std::size_t{6} * slotCapacity_ + 16);
        fifo_.assign(fifoCap, Pending{});
        head_ = count_ = 0;
        headSeq_ = 0;
        assignSeq_.assign(slotCapacity_, kNone);
        deletingSeq_.assign(slotCapacity_, kNone);
        assignNs_.assign(slotCapacity_, 0);
        assignNsValid_.assign(slotCapacity_, 0);
        const std::size_t cells = static_cast<std::size_t>(slotCapacity_) * kKinds;
        latest_.assign(cells, Latest{});
        dirty_.assign(cells * workers_, 0);
        dirtyList_.assign(cells * workers_, 0);
        dirtyCount_.assign(workers_, 0);
        deletingNs_.assign(slotCapacity_, 0);
        deletingPosted_.assign(slotCapacity_, 0);
    }

    void post(const NsSlotAssign& m) {
        if (m.slot >= slotCapacity_) [[unlikely]] { (void)postOrdered_(Msg{m}, false); return; }   // guard
        if (deletingPosted_[m.slot] != 0 && deletingNs_[m.slot] != m.nsId)
            deletingPosted_[m.slot] = 0;                       // a new owner: its own NsDeleting must go out
        const bool sameOwnerBefore = assignNsValid_[m.slot] != 0 && assignNs_[m.slot] == m.nsId;
        if (Pending* e = unseen_(assignSeq_[m.slot])) {
            // sameOwnerBefore: no Release of this owner is queued behind the entry (a Release clears it).
            if (!e->dead && sameOwnerBefore && std::get<NsSlotAssign>(e->msg).nsId == m.nsId) {   // a limits change
                std::get<NsSlotAssign>(e->msg) = m;
                ++assignMerges_;
                return;
            }
            if (e->dead) {                                     // revive: it sits behind every live entry of this slot
                e->msg     = Msg{m};
                e->dead    = false;
                e->refresh = false;
                assignNs_[m.slot] = m.nsId;
                assignNsValid_[m.slot] = 1;
                return;
            }
        }
        uint64_t seq = 0;
        if (postOrdered_(Msg{m}, sameOwnerBefore, &seq)) {
            assignSeq_[m.slot] = seq;
            assignNs_[m.slot] = m.nsId;
            assignNsValid_[m.slot] = 1;
        }
    }
    void post(const NsSlotRelease& m) {
        if (m.slot < deletingPosted_.size()) deletingPosted_[m.slot] = 0;
        if (m.slot < slotCapacity_)                              // #866: the old owner's owed throttles die with it
            for (std::size_t k = 0; k < kKinds; ++k) latest_[static_cast<std::size_t>(m.slot) * kKinds + k].live = false;
        if (m.slot < slotCapacity_) {
            assignNsValid_[m.slot] = 0;
            Pending* a = unseen_(assignSeq_[m.slot]);
            if (a != nullptr && !a->dead && !a->refresh && std::get<NsSlotAssign>(a->msg).nsId == m.nsId) {
                // No worker ever learned this owner: its Assign and NsDeleting die unseen and no Release is queued.
                a->dead = true;
                Pending* d = unseen_(deletingSeq_[m.slot]);
                if (d != nullptr && !d->dead && std::get<Throttle>(d->msg).nsId == m.nsId) d->dead = true;
                ++cyclesCancelled_;
                return;
            }
        }
        (void)postOrdered_(Msg{m}, false);
    }
    void post(const Throttle& m) {   // Task 14
        if (m.kind == ThrottleKind::NsDeleting) {              // sticky, not a rate: ordered, once per slot owner (#864)
            if (m.slot >= deletingPosted_.size()) [[unlikely]] { ++badPosts_; return; }
            if (deletingPosted_[m.slot] != 0 && deletingNs_[m.slot] == m.nsId) { ++deletingDedups_; return; }
            if (Pending* e = unseen_(deletingSeq_[m.slot]); e != nullptr && e->dead) {
                // Revive only when it stays behind the Assign of its owner.
                const uint64_t as = assignSeq_[m.slot];
                const bool assignQueued = as != kNone && as >= headSeq_ && as < headSeq_ + count_;
                if (!assignQueued || deletingSeq_[m.slot] > as) {
                    e->msg  = Msg{m};
                    e->dead = false;
                    deletingPosted_[m.slot] = 1; deletingNs_[m.slot] = m.nsId;
                    return;
                }
            }
            uint64_t seq = 0;
            if (postOrdered_(Msg{m}, false, &seq)) {
                deletingSeq_[m.slot] = seq;
                deletingPosted_[m.slot] = 1; deletingNs_[m.slot] = m.nsId;
            }
            return;
        }
        const std::size_t k = static_cast<std::size_t>(m.kind);
        if (k >= kThrottleKinds) [[unlikely]] { ++badPosts_; return; }
        postLatest_(m.slot, k, m.nsId, m.untilNs);
    }
    void post(const ViewMemOver& m) { postLatest_(m.slot, kViewMemOverKind, m.nsId, m.on ? 1 : 0); }   // Task 14
    void post(const LeasesOver& m) { postLatest_(m.slot, kLeasesOverKind, m.nsId, m.on ? 1 : 0); }     // Lease topics Task 21

    /// Control thread, every duty cycle.  Returns the number of ops enqueued.
    template <class Pool>
    int pump(Pool& pool) {
        int work = 0;
        const int32_t n = pool.workerCount();
        // 1. The ordered FIFO, message by message (a message that cannot reach every worker stalls the ones behind it).
        std::size_t done = 0;
        for (std::size_t i = 0; i < count_; ++i) {
            Pending& p = fifo_[(head_ + i) % fifo_.size()];
            if (p.dead) { ++done; continue; }              // cancelled before any worker saw it: sends nothing
            while (p.nextWorker < n) {
                WorkerOp op = std::visit(
                    [](const auto& m) {
                        using T = std::decay_t<decltype(m)>;
                        if constexpr (std::is_same_v<T, NsSlotAssign>) return makeNsSlotAssignOp(m);
                        else if constexpr (std::is_same_v<T, NsSlotRelease>) return makeNsSlotReleaseOp(m);
                        else return makeThrottleOp(m);
                    }, p.msg);
                const EnqueueResult r = pool.worker(p.nextWorker).enqueueOp(std::move(op));
                if (r == EnqueueResult::Full) break;   // back-pressure: retry next cycle (FIFO)
                if (r == EnqueueResult::Enqueued) { ++enqueued_; ++work; }
                else [[unlikely]] {
                    ++tooLargeDrops_;
                    spdlog::error("NsSlotFanout: a namespace slot message cannot be enqueued on worker {} (TooLarge)",
                                  p.nextWorker);
                }
                ++p.nextWorker;
            }
            if (p.nextWorker < n) break;               // FIFO
            ++done;
        }
        head_ = (head_ + done) % fifo_.size();
        count_ -= done;
        headSeq_ += done;
        // 2. The latest-wins table, per worker, once that worker has no ordered message pending.  Every remaining FIFO
        //    entry still owes workers >= its nextWorker, so worker w is clear iff all of them have nextWorker > w.
        int32_t minNext = n;
        for (std::size_t i = 0; i < count_; ++i) {
            const Pending& q = fifo_[(head_ + i) % fifo_.size()];
            if (!q.dead) minNext = std::min(minNext, q.nextWorker);
        }
        const std::size_t cells = static_cast<std::size_t>(slotCapacity_) * kKinds;
        for (int32_t w = 0; w < n && static_cast<uint32_t>(w) < workers_ && w < minNext; ++w) {
            uint32_t& cnt = dirtyCount_[static_cast<std::size_t>(w)];
            if (cnt == 0) continue;
            uint32_t* list = dirtyList_.data() + static_cast<std::size_t>(w) * cells;
            uint8_t*  flag = dirty_.data() + static_cast<std::size_t>(w) * cells;
            uint32_t kept = 0;
            bool ringFull = false;
            for (uint32_t i = 0; i < cnt; ++i) {
                const uint32_t cell = list[i];
                if (ringFull) { list[kept++] = cell; continue; }   // keep the rest dirty, in order
                const Latest& l = latest_[cell];
                if (!l.live) { flag[cell] = 0; ++voidDrops_; continue; }   // released since it was posted: nothing to send
                const uint16_t slot = static_cast<uint16_t>(cell / kKinds);
                const std::size_t kind = cell % kKinds;
                WorkerOp op = kind == kViewMemOverKind ? makeViewMemOverOp(ViewMemOver{slot, l.nsId, l.value != 0})
                            : kind == kLeasesOverKind  ? makeLeasesOverOp(LeasesOver{slot, l.nsId, l.value != 0})
                            : makeThrottleOp(Throttle{slot, l.nsId, static_cast<ThrottleKind>(kind), l.value});
                const EnqueueResult r = pool.worker(w).enqueueOp(std::move(op));
                if (r == EnqueueResult::Full) { ringFull = true; list[kept++] = cell; continue; }
                if (r == EnqueueResult::Enqueued) { ++enqueued_; ++work; }
                else [[unlikely]] {
                    ++tooLargeDrops_;
                    spdlog::error("NsSlotFanout: a namespace throttle cannot be enqueued on worker {} (TooLarge)", w);
                }
                flag[cell] = 0;
            }
            cnt = kept;
        }
        return work;
    }

    /// Ordered messages still pending (test / observability).
    [[nodiscard]] std::size_t pendingForTest() const noexcept { return count_; }
    [[nodiscard]] uint64_t    enqueuedForTest() const noexcept { return enqueued_; }
    /// Latest-wins entries still owed to `worker`.
    [[nodiscard]] uint32_t    dirtyForTest(int32_t worker) const noexcept {
        return worker >= 0 && static_cast<std::size_t>(worker) < dirtyCount_.size() ? dirtyCount_[static_cast<std::size_t>(worker)] : 0;
    }
    [[nodiscard]] uint64_t    orderedOverflowsForTest() const noexcept { return orderedOverflows_; }
    [[nodiscard]] uint64_t    deletingDedupsForTest() const noexcept { return deletingDedups_; }
    [[nodiscard]] uint64_t    voidDropsForTest() const noexcept { return voidDrops_; }
    [[nodiscard]] uint64_t    assignMergesForTest() const noexcept { return assignMerges_; }
    [[nodiscard]] uint64_t    cyclesCancelledForTest() const noexcept { return cyclesCancelled_; }

private:
    using Msg = std::variant<NsSlotAssign, NsSlotRelease, Throttle>;
    static constexpr uint64_t kNone = UINT64_MAX;
    struct Pending {
        Msg msg{NsSlotRelease{}};
        int32_t nextWorker = 0;
        bool dead = false;      ///< cancelled before any worker saw it: pump skips it
        bool refresh = false;   ///< an Assign of an owner an earlier Assign already announced (a limits change)
    };
    struct Latest  { uint32_t nsId = 0; int64_t value = 0; bool live = false; };

    /// The queued entry with sequence `seq`, only while no worker has seen it (nextWorker == 0); else nullptr.
    Pending* unseen_(uint64_t seq) noexcept {
        if (seq == kNone || seq < headSeq_ || seq >= headSeq_ + count_) return nullptr;
        Pending& p = fifo_[(head_ + static_cast<std::size_t>(seq - headSeq_)) % fifo_.size()];
        return p.nextWorker == 0 ? &p : nullptr;
    }
    bool postOrdered_(Msg m, bool refresh, uint64_t* seqOut = nullptr) {
        if (count_ == fifo_.size()) [[unlikely]] {
            ++orderedOverflows_;
            spdlog::critical("NsSlotFanout: the ordered namespace-slot FIFO is full ({} messages): a message is lost (bug; "
                             "it holds a bounded number of entries per slot)", fifo_.size());
            return false;
        }
        if (seqOut != nullptr) *seqOut = headSeq_ + count_;
        fifo_[(head_ + count_) % fifo_.size()] = Pending{std::move(m), 0, false, refresh};
        ++count_;
        return true;
    }
    void postLatest_(uint16_t slot, std::size_t kind, uint32_t nsId, int64_t value) {
        if (slot >= slotCapacity_) [[unlikely]] { ++badPosts_; return; }
        const std::size_t cells = static_cast<std::size_t>(slotCapacity_) * kKinds;
        const std::size_t cell  = static_cast<std::size_t>(slot) * kKinds + kind;
        latest_[cell] = Latest{nsId, value, true};
        for (uint32_t w = 0; w < workers_; ++w) {
            uint8_t& f = dirty_[w * cells + cell];
            if (f != 0) continue;   // already owed: it will carry the latest value
            f = 1;
            dirtyList_[w * cells + dirtyCount_[w]++] = static_cast<uint32_t>(cell);
        }
    }

    uint32_t slotCapacity_ = 0;
    uint32_t workers_      = 1;
    std::vector<Pending>  fifo_;
    std::size_t           head_ = 0, count_ = 0;
    uint64_t              headSeq_ = 0;     ///< sequence number of fifo_[head_]
    std::vector<uint64_t> assignSeq_;       ///< [slot]: sequence of the newest queued Assign (live or dead)
    std::vector<uint64_t> deletingSeq_;     ///< [slot]: sequence of the newest queued NsDeleting entry (live or dead)
    std::vector<uint32_t> assignNs_;        ///< [slot]: the owner of the last posted Assign
    std::vector<uint8_t>  assignNsValid_;   ///< [slot]: 1 = assignNs_ is valid (cleared by a Release)
    uint64_t assignMerges_ = 0;
    uint64_t cyclesCancelled_ = 0;
    std::vector<Latest>   latest_;
    std::vector<uint8_t>  dirty_;        ///< [worker][cell]
    std::vector<uint32_t> dirtyList_;    ///< [worker][0 .. dirtyCount_[worker]) cells, first-dirty order
    std::vector<uint32_t> dirtyCount_;
    uint64_t enqueued_ = 0;
    uint64_t tooLargeDrops_ = 0;
    uint64_t orderedOverflows_ = 0;
    uint64_t badPosts_ = 0;
    std::vector<uint32_t> deletingNs_;      ///< [slot]: the owner whose NsDeleting is already queued or delivered
    std::vector<uint8_t>  deletingPosted_;  ///< [slot]: 1 = deletingNs_[slot] is valid
    uint64_t deletingDedups_ = 0;
    uint64_t voidDrops_ = 0;
};

}  // namespace rune::node
