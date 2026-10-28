// NsLimitAggregator.cpp - see NsLimitAggregator.h.
#include <rune/node/NsLimitAggregator.h>

#include <algorithm>
#include <cstdlib>

namespace rune::node {

namespace {
struct RateRow { ThrottleKind kind; NsMetric metric; uint64_t rune::protocol::NamespaceLimits::*limit; };
constexpr RateRow kRates[] = {
    {ThrottleKind::ProduceBytes,   NsMetric::ProduceBytes,   &rune::protocol::NamespaceLimits::produceBytesPerSec},
    {ThrottleKind::ProduceRecords, NsMetric::ProduceRecords, &rune::protocol::NamespaceLimits::produceRecordsPerSec},
    {ThrottleKind::FetchBytes,     NsMetric::FetchBytes,     &rune::protocol::NamespaceLimits::fetchBytesPerSec},
    {ThrottleKind::RequestOps,     NsMetric::RequestOps,     &rune::protocol::NamespaceLimits::requestOpsPerSec},
    {ThrottleKind::QueryCycles,    NsMetric::QueryCycles,    &rune::protocol::NamespaceLimits::queryCyclesPerSec},
};
}  // namespace

void NsLimitAggregator::resetSlot_(uint16_t slot) noexcept {
    const std::size_t s = slot;
    for (std::size_t m = 0; m < kNsMetrics; ++m) { total_[s * kNsMetrics + m] = 0; prevTotal_[s * kNsMetrics + m] = 0; rate_[s * kNsMetrics + m] = 0; }
    for (std::size_t src = 0; src < sources_; ++src)
        for (std::size_t m = 0; m < kNsMetrics; ++m) last_[(s * sources_ + src) * kNsMetrics + m] = 0;
    for (std::size_t k = 0; k < kThrottleKinds; ++k) { debt_[s * kThrottleKinds + k] = 0; sentUntil_[s * kThrottleKinds + k] = 0; }
    sentViewOver_[s] = 0;
}

void NsLimitAggregator::emitThrottle_(uint16_t slot, ThrottleKind k, int64_t until) {
    sentUntil_[static_cast<std::size_t>(slot) * kThrottleKinds + static_cast<std::size_t>(k)] = until;
    Event e{};
    e.isThrottle = true;
    e.throttle   = Throttle{slot, slots_.nsIdAt(slot), k, until};
    if (events_.size() < events_.capacity()) events_.push_back(e);   // capacity covers every slot x kind: never drops
}

int NsLimitAggregator::doWork(int64_t nowNs) {
    if (!started_) { started_ = true; lastTickNs_ = nowNs; return 0; }   // the first call only sets the baseline
    if (nowNs - lastTickNs_ < tickNs_) return 0;
    const int64_t elapsed = nowNs - lastTickNs_;
    lastTickNs_ = nowNs;
    const std::size_t before = events_.size();
    const uint32_t cap = slots_.capacity();
    for (uint32_t su = 0; su < cap; ++su) {
        const auto slot = static_cast<uint16_t>(su);
        if (slots_.nsIdAt(slot) == NsSlotTable::kFreeSlot) continue;
        const rune::protocol::NamespaceLimits& L = slots_.limits(slot);
        const std::size_t s = su;

        // ---- rates (D28) ----
        for (const RateRow& r : kRates) {
            const std::size_t mi = s * kNsMetrics + static_cast<std::size_t>(r.metric);
            const std::size_t ki = s * kThrottleKinds + static_cast<std::size_t>(r.kind);
            const uint64_t delta = total_[mi] - prevTotal_[mi];
            prevTotal_[mi] = total_[mi];
            rate_[mi] = static_cast<uint64_t>((static_cast<__uint128_t>(delta) * 1'000'000'000ull) / static_cast<uint64_t>(elapsed));
            const uint64_t limit = L.*(r.limit);
            int64_t until = 0;
            if (limit == 0) {
                debt_[ki] = 0;   // unlimited
            } else {
                const auto allowed = static_cast<uint64_t>((static_cast<__uint128_t>(limit) * static_cast<uint64_t>(elapsed)) / 1'000'000'000ull);
                const uint64_t owed = debt_[ki] + delta;
                uint64_t debt = owed > allowed ? owed - allowed : 0;
                const uint64_t maxDebt = static_cast<uint64_t>((static_cast<__uint128_t>(limit) * static_cast<uint64_t>(kMaxThrottleNs)) / 1'000'000'000ull);
                debt = std::min(debt, std::max<uint64_t>(maxDebt, 1));
                debt_[ki] = debt;
                if (debt > 0) {
                    const auto wait = static_cast<int64_t>(std::min<__uint128_t>(
                        (static_cast<__uint128_t>(debt) * 1'000'000'000ull + limit - 1) / limit, static_cast<__uint128_t>(kMaxThrottleNs)));
                    until = nowNs + std::max<int64_t>(wait, 1);
                }
            }
            const int64_t sent = sentUntil_[ki];
            const bool changed = (until == 0) != (sent == 0) || (until != 0 && std::abs(until - sent) > kRebroadcastSlackNs);
            if (changed) emitThrottle_(slot, r.kind, until);
        }

        // ---- disk (D25): the node total ----
        {
            const std::size_t ki = s * kThrottleKinds + static_cast<std::size_t>(ThrottleKind::Disk);
            const uint64_t used = total_[s * kNsMetrics + static_cast<std::size_t>(NsMetric::DiskBytes)];
            const int64_t until = (L.maxDiskBytes != 0 && used >= L.maxDiskBytes) ? std::numeric_limits<int64_t>::max() : 0;
            if (until != sentUntil_[ki]) emitThrottle_(slot, ThrottleKind::Disk, until);
        }

        // ---- view memory (D26): the node total ----
        {
            const uint64_t used = total_[s * kNsMetrics + static_cast<std::size_t>(NsMetric::ViewMemBytes)];
            const uint8_t over = (L.maxViewMemoryBytes != 0 && used > L.maxViewMemoryBytes) ? 1 : 0;
            if (over != sentViewOver_[s]) {
                sentViewOver_[s] = over;
                Event e{};
                e.isThrottle = false;
                e.viewMem    = ViewMemOver{slot, slots_.nsIdAt(slot), over != 0};
                if (events_.size() < events_.capacity()) events_.push_back(e);
            }
        }
    }
    return static_cast<int>(events_.size() - before);
}

}  // namespace rune::node
