#pragma once
// NsLimitAggregator.h - the per-namespace limit aggregator (namespaces Phase 2 Task 14, D25/D26/D28).
//
// Owned by the CONTROL thread (shared-nothing: no other thread touches it).  Every thread that counts something
// (the gateway, every worker) reports its own cumulative counters / current gauges over the existing rings; the
// aggregator sums them per namespace slot and, every RUNE_NS_LIMIT_TICK_US, compares them with the slot's limits:
//
//   rates  (produce bytes / records, fetch bytes, request ops, query cycles) - D28: the slot's DEBT is the excess over
//          limit x elapsed carried between ticks (debt = max(0, debt + delta - limit*elapsed)); while debt > 0 the slot is
//          throttled until now + debt / limit.  The deadline is NOT rebroadcast while it only decays with time (a
//          throttled client sends nothing, the debt falls by exactly limit*elapsed, now + debt/limit is constant); it is
//          broadcast when it appears, moves by more than kRebroadcastSlackNs, or clears.  Convergence needs no timer
//          constant: the clear is derived from the counters of the next tick (rule 16).
//   disk   (D25) - the node total across workers; at or over maxDiskBytes -> Throttle{Disk, INT64_MAX}, else 0.
//   view memory (D26) - the node total; over maxViewMemoryBytes -> ViewMemOver{on}, else off.
//
// Broadcast on change only.  Every message carries (slot, nsId); a report or a limit refresh for an nsId that the slot does
// not hold is dropped and counted (staleReports).  Zero allocation after configure(): every table is sized there.
//
// Time: the injected monotonic clock only (doWork(nowNs)); no wall time.

#include <rune/node/NsRingMessages.h>
#include <rune/node/NsSlotTable.h>
#include <rune/protocol/NamespaceLimits.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace rune::node {

/// What a reporter measures.  Rates are cumulative counters; DiskBytes / ViewMemBytes are gauges (current bytes).
enum class NsMetric : uint8_t { ProduceBytes = 0, ProduceRecords = 1, FetchBytes = 2, RequestOps = 3, QueryCycles = 4,
                                DiskBytes = 5, ViewMemBytes = 6, kCount = 7 };
inline constexpr std::size_t kNsMetrics = static_cast<std::size_t>(NsMetric::kCount);

class NsLimitAggregator {
public:
    /// A deadline that moves by less than this is the same deadline (integer rounding of a decaying debt).
    static constexpr int64_t kRebroadcastSlackNs = 1'000;
    /// The longest a rate throttle is ever set for (a pathological burst must not lock a namespace out for minutes).
    static constexpr int64_t kMaxThrottleNs = 10'000'000'000LL;

    NsLimitAggregator() = default;

    /// Cold, startup: `slotCapacity` namespace slots, `sources` reporters (workers + the gateway), a tick of `tickNs`.
    void configure(uint32_t slotCapacity, uint32_t sources, int64_t tickNs) {
        slots_.reset(slotCapacity);
        sources_ = sources < 1 ? 1 : sources;
        tickNs_  = tickNs < 1 ? 1 : tickNs;
        const std::size_t cap = slots_.capacity();
        last_.assign(cap * sources_ * kNsMetrics, 0);
        total_.assign(cap * kNsMetrics, 0);
        prevTotal_.assign(cap * kNsMetrics, 0);
        debt_.assign(cap * kThrottleKinds, 0);
        sentUntil_.assign(cap * kThrottleKinds, 0);
        sentViewOver_.assign(cap, 0);
        events_.clear();
        events_.reserve(cap * (kThrottleKinds + 1));
        started_ = false;
    }

    [[nodiscard]] uint32_t slotCapacity() const noexcept { return slots_.capacity(); }
    [[nodiscard]] int64_t  tickNs() const noexcept { return tickNs_; }

    /// A slot gained an owner, or its owner's limits changed.  A NEW owner starts from zero usage and zero state.
    void onSlotAssign(uint16_t slot, uint32_t nsId, const rune::protocol::NamespaceLimits& l) {
        if (slots_.nsIdAt(slot) != nsId) {
            if (!slots_.place(slot, nsId)) { ++staleReports_; return; }
            resetSlot_(slot);
        }
        slots_.setLimits(slot, l);   // the next tick applies them (a SetLimits between a read and a broadcast)
    }
    void onSlotRelease(uint16_t slot, uint32_t nsId) {
        if (!slots_.release(slot, nsId)) { ++staleReports_; return; }
        resetSlot_(slot);
    }

    /// A reporter's value of one metric (cumulative for the rates, current for the gauges).  False (counted, dropped)
    /// when the slot does not hold `nsId` - a report in flight across a delete / recreate never lands on the new owner.
    bool report(uint32_t source, uint16_t slot, uint32_t nsId, NsMetric metric, uint64_t value) noexcept {
        if (!slots_.matches(slot, nsId) || source >= sources_) { ++staleReports_; return false; }
        const std::size_t m = static_cast<std::size_t>(metric);
        uint64_t& last = last_[(static_cast<std::size_t>(slot) * sources_ + source) * kNsMetrics + m];
        uint64_t& tot  = total_[static_cast<std::size_t>(slot) * kNsMetrics + m];
        tot = tot + value - last;   // unsigned wrap is exact: tot always equals the sum of the sources' last values
        last = value;
        return true;
    }

    /// One tick per tickNs; zero allocation.  Returns the number of messages produced.
    int doWork(int64_t nowNs);

    /// The messages produced since the last drain, in order.  `f(const Throttle&)` / `g(const ViewMemOver&)`.
    template <class F, class G>
    void drain(F&& f, G&& g) {
        for (const Event& e : events_) {
            if (e.isThrottle) f(e.throttle);
            else g(e.viewMem);
        }
        events_.clear();
    }
    [[nodiscard]] std::size_t pending() const noexcept { return events_.size(); }

    [[nodiscard]] uint64_t staleReports() const noexcept { return staleReports_; }
    [[nodiscard]] uint64_t totalOf(uint16_t slot, NsMetric m) const noexcept {
        return slot < slots_.capacity() ? total_[static_cast<std::size_t>(slot) * kNsMetrics + static_cast<std::size_t>(m)] : 0;
    }
    [[nodiscard]] uint64_t debtOf(uint16_t slot, ThrottleKind k) const noexcept {
        return slot < slots_.capacity() ? debt_[static_cast<std::size_t>(slot) * kThrottleKinds + static_cast<std::size_t>(k)] : 0;
    }
    [[nodiscard]] int64_t sentUntil(uint16_t slot, ThrottleKind k) const noexcept {
        return slot < slots_.capacity() ? sentUntil_[static_cast<std::size_t>(slot) * kThrottleKinds + static_cast<std::size_t>(k)] : 0;
    }
    [[nodiscard]] const NsSlotTable& slotsForTest() const noexcept { return slots_; }

private:
    struct Event {
        bool isThrottle;
        Throttle throttle;
        ViewMemOver viewMem;
    };
    void resetSlot_(uint16_t slot) noexcept;
    void emitThrottle_(uint16_t slot, ThrottleKind k, int64_t until);

    NsSlotTable slots_{1};
    uint32_t    sources_ = 1;
    int64_t     tickNs_  = 10'000'000;
    int64_t     lastTickNs_ = 0;
    bool        started_ = false;
    uint64_t    staleReports_ = 0;
    std::vector<uint64_t> last_;        // [slot][source][metric]
    std::vector<uint64_t> total_;       // [slot][metric]
    std::vector<uint64_t> prevTotal_;   // [slot][metric] at the previous tick (rates)
    std::vector<uint64_t> debt_;        // [slot][throttle kind]
    std::vector<int64_t>  sentUntil_;   // [slot][throttle kind] the last broadcast deadline
    std::vector<uint8_t>  sentViewOver_;
    std::vector<Event>    events_;
};

}  // namespace rune::node
