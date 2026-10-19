// WaiterQueueBench.cpp — F15 S5 fix round 2 (re-review 1 I-2 + "valid
// measurement"): the parked-waiter container IN ISOLATION (no I/O, no worker),
// all structures in ONE binary (no cross-build layout variance):
//   Vec  — S5 e69a215: std::vector + order-preserving erase (removeInOrder_)
//   R1   — fix round 1 d532368: OrderedWaitQueue with in-slot live flag, compaction only when full
//   Q    — HEAD: OrderedWaitQueue with a dense live array + eager compaction (span <= 2·live + slack)
// Element: a PendingAcksAll-shaped waiter (~200 B incl. a WorkerReturn).
// Production reserve: Worker::kMaxOwedReturns (4096).
//
// Arms (Args {mode, n}):
//   mode 0 = front fire: n waiters of one partition parked; each pass parks 16
//            and fires the 16 oldest (the HW is monotone);
//   mode 1 = stalled front: ONE never-firing waiter parked first (a lagging
//            follower's partition), then n live waiters of another partition
//            behave as in mode 0.
// Per pass: p50 / p99 / max (ns) reported as counters, so compaction spikes show.
#include "BenchGate.h"
#include <rune/node/OrderedWaitQueue.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>

#include <benchmark/benchmark.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace {

using rune::node::WorkerReturn;

struct Waiter {
    void*        replica    = nullptr;
    int64_t      lastOffset = 0;
    int64_t      deadlineNs = 0;
    uint64_t     logGen     = 0;
    int32_t      minIsr     = 1;
    int32_t      partition  = 0;
    WorkerReturn ret;
};

template <typename T>
class OrderedWaitQueueR1 {
public:
    /// Reserve room for `n` LIVE entries (ring = 2n slots).  Cold.
    void reserve(std::size_t n) {
        const std::size_t want = pow2_(n * 2);
        if (want <= slots_.size()) return;
        regrow_(want);
    }

    [[nodiscard]] std::size_t size() const noexcept { return live_; }
    [[nodiscard]] bool empty() const noexcept { return live_ == 0; }
    [[nodiscard]] std::size_t capacitySlots() const noexcept { return slots_.size(); }
    [[nodiscard]] uint64_t growths() const noexcept { return growths_; }
    [[nodiscard]] uint64_t compactions() const noexcept { return compactions_; }

    void push_back(T&& v) {
        if (tail_ - head_ == slots_.size()) [[unlikely]] makeRoom_();
        Slot& s = slots_[idx_(tail_)];
        s.v = std::move(v);
        s.live = true;
        ++tail_;
        ++live_;
    }

    /// Visit live entries in park order; `take(T&)` returning true removes the
    /// entry (the visitor has fired / failed it).  Returns the number removed.
    /// The visitor must not push into THIS queue.
    template <typename Take>
    std::size_t sweep(Take&& take) {
        std::size_t removed = 0;
        for (uint64_t i = head_; i < tail_; ++i) {
            Slot& s = slots_[idx_(i)];
            if (!s.live) continue;
            if (!take(s.v)) continue;
            s.live = false;
            s.v = T{};          // release payload resources now (cold for POD-ish T)
            --live_;
            ++removed;
        }
        popDeadFront_();
        return removed;
    }

    /// Visit live entries in park order (read / in-place update, no removal).
    template <typename Fn>
    void forEach(Fn&& fn) {
        for (uint64_t i = head_; i < tail_; ++i) {
            Slot& s = slots_[idx_(i)];
            if (s.live) fn(s.v);
        }
    }
    template <typename Fn>
    void forEach(Fn&& fn) const {
        for (uint64_t i = head_; i < tail_; ++i) {
            const Slot& s = slots_[idx_(i)];
            if (s.live) fn(s.v);
        }
    }

    /// The k-th live entry in park order (tests; O(n)).
    [[nodiscard]] const T& at(std::size_t k) const {
        for (uint64_t i = head_; i < tail_; ++i) {
            const Slot& s = slots_[idx_(i)];
            if (s.live && k-- == 0) return s.v;
        }
        throw std::out_of_range("OrderedWaitQueue::at");
    }

private:
    struct Slot {
        T    v{};
        bool live = false;
    };
    [[nodiscard]] std::size_t idx_(uint64_t i) const noexcept { return static_cast<std::size_t>(i) & mask_; }
    static std::size_t pow2_(std::size_t n) noexcept {
        std::size_t p = 16;
        while (p < n) p <<= 1;
        return p;
    }
    void popDeadFront_() noexcept {
        while (head_ < tail_ && !slots_[idx_(head_)].live) ++head_;
        if (head_ == tail_) head_ = tail_ = 0;   // empty: restart at slot 0 (cache locality)
    }
    void makeRoom_() {
        if (slots_.empty()) { regrow_(16); return; }
        if (live_ < slots_.size()) { compact_(); if (tail_ - head_ < slots_.size()) return; }
        ++growths_;
        regrow_(slots_.size() * 2);   // cold: only past the reserved capacity
    }
    // Close the tombstone gaps in place, keeping order (O(n), cold/amortised).
    void compact_() {
        ++compactions_;
        uint64_t w = head_;
        for (uint64_t r = head_; r < tail_; ++r) {
            Slot& s = slots_[idx_(r)];
            if (!s.live) continue;
            if (w != r) {
                Slot& d = slots_[idx_(w)];
                d.v = std::move(s.v);
                d.live = true;
                s.v = T{};
                s.live = false;
            }
            ++w;
        }
        tail_ = w;
    }
    void regrow_(std::size_t n) {
        n = pow2_(n);
        std::vector<Slot> next(n);
        std::size_t k = 0;
        for (uint64_t i = head_; i < tail_; ++i) {
            Slot& s = slots_[idx_(i)];
            if (!s.live) continue;
            next[k].v = std::move(s.v);
            next[k].live = true;
            ++k;
        }
        slots_ = std::move(next);
        mask_ = slots_.size() - 1;
        head_ = 0;
        tail_ = k;
    }

    std::vector<Slot> slots_;
    std::size_t mask_ = 0;   ///< slots_.size() - 1 (power of two)
    uint64_t head_ = 0;
    uint64_t tail_ = 0;
    std::size_t live_ = 0;
    uint64_t growths_ = 0;
    uint64_t compactions_ = 0;
};


// S5's vector + order-preserving erase.
struct VecQueue {
    std::vector<Waiter> v;
    void reserve(std::size_t n) { v.reserve(n); }
    void push_back(Waiter&& w) { v.push_back(std::move(w)); }
    std::size_t size() const { return v.size(); }
    template <typename Take>
    std::size_t sweep(Take&& take) {
        std::size_t w = 0;
        const std::size_t n = v.size();
        for (std::size_t r = 0; r < n; ++r) {
            if (take(v[r])) continue;
            if (w != r) v[w] = std::move(v[r]);
            ++w;
        }
        const std::size_t removed = n - w;
        v.erase(v.begin() + static_cast<std::ptrdiff_t>(w), v.end());
        return removed;
    }
};

template <typename Q>
void runWaiterQueue(benchmark::State& state) {
    const bool stalled = state.range(0) != 0;
    const int64_t n = state.range(1);
    constexpr int kFire = 16;
    Q q;
    q.reserve(rune::node::Worker::kMaxOwedReturns);
    int64_t nextOff = 0;
    auto park = [&](int32_t part, int64_t off) {
        Waiter w;
        w.partition = part; w.lastOffset = off; w.ret.correlationId = off;
        q.push_back(std::move(w));
    };
    if (stalled) park(1, 0);                                  // never fires (partition 1 HW stays 0)
    for (int64_t i = 0; i < n; ++i) park(0, nextOff++);
    int64_t hw0 = 0;                                          // partition 0 HW: fires lastOffset < hw0
    int64_t fired = 0;
    std::vector<int64_t> ns;
    ns.reserve(1 << 20);
    for (auto _ : state) {
        const auto t0 = std::chrono::steady_clock::now();
        for (int k = 0; k < kFire; ++k) park(0, nextOff++);
        hw0 = nextOff - n;                                    // the kFire oldest of partition 0 pass the HW
        fired += static_cast<int64_t>(q.sweep([&](Waiter& w) {
            if (w.partition != 0 || w.lastOffset >= hw0) return false;
            benchmark::DoNotOptimize(w.ret.correlationId);
            return true;
        }));
        const auto t1 = std::chrono::steady_clock::now();
        if (ns.size() < ns.capacity())
            ns.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    }
    if (fired != static_cast<int64_t>(state.iterations()) * kFire) rune::bench::gateFail(state, "fired count mismatch");
    if (static_cast<int64_t>(q.size()) != n + (stalled ? 1 : 0)) rune::bench::gateFail(state, "live count drifted");
    std::sort(ns.begin(), ns.end());
    if (!ns.empty()) {
        state.counters["p50ns"] = static_cast<double>(ns[ns.size() / 2]);
        state.counters["p99ns"] = static_cast<double>(ns[ns.size() * 99 / 100]);
        state.counters["maxns"] = static_cast<double>(ns.back());
    }
    state.SetItemsProcessed(state.iterations() * kFire);
}

void BM_WaiterQueue_Vec(benchmark::State& s) { runWaiterQueue<VecQueue>(s); }
void BM_WaiterQueue_R1(benchmark::State& s)  { runWaiterQueue<OrderedWaitQueueR1<Waiter>>(s); }
void BM_WaiterQueue_Q(benchmark::State& s)   { runWaiterQueue<rune::node::OrderedWaitQueue<Waiter>>(s); }

#define WAITER_QUEUE_ARGS \
    ->Args({0, 64})->Args({0, 512})->Args({0, 4079}) \
    ->Args({1, 16})->Args({1, 64})->Args({1, 512}) \
    ->Unit(benchmark::kNanosecond)->MinTime(0.5)
BENCHMARK(BM_WaiterQueue_Vec) WAITER_QUEUE_ARGS;
BENCHMARK(BM_WaiterQueue_R1) WAITER_QUEUE_ARGS;
BENCHMARK(BM_WaiterQueue_Q) WAITER_QUEUE_ARGS;

} // namespace
