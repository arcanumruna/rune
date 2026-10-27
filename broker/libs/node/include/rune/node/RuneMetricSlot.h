// ---------------------------------------------------------------------------
// RuneMetricSlot.h - one kRuneMetrics row's writer, bound to its declared MetricOwner (arcanum-metrics D58).
//
// A trivially copyable value: the arcanum-metrics SlotWriter (a release store into the mmapped value word) plus, for
// the debug owner-tag check, the row's declared owner and a pointer to its "written by" record.  Each writer passes
// its own owner tag on every store; a debug build records the tag (one bit per MetricOwner) and asserts it equals
// the row's declared owner.  No thread_local, no virtual call; a release build compiles the check out, leaving the
// single release store.
//
// The record is diagnostics only (never read on a hot path, never used to coordinate): BrokerNode owns one byte per
// row; each byte has exactly one writer thread while the owner table holds, and the test thread reads it through
// std::atomic_ref.
// ---------------------------------------------------------------------------
#ifndef RUNE_NODE_RUNE_METRIC_SLOT_H
#define RUNE_NODE_RUNE_METRIC_SLOT_H

#include <rune/node/RuneMetrics.h>

#include <metrics/MetricsWriter.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <type_traits>

namespace rune::node {

/// The record bit of an owner: (1 << owner).
[[nodiscard]] constexpr std::uint8_t metricOwnerBit(MetricOwner o) noexcept {
    return static_cast<std::uint8_t>(1u << static_cast<unsigned>(o));
}

class RuneMetricSlot {
public:
    RuneMetricSlot() noexcept = default;
    RuneMetricSlot(metrics::SlotWriter w, MetricOwner owner, std::uint8_t* writtenBy) noexcept
        : w_(w), writtenBy_(writtenBy), owner_(owner) {}

    /// False for a default-constructed (unbound) slot: nothing is published through it.
    [[nodiscard]] bool bound() const noexcept { return w_.slot() >= 0; }
    [[nodiscard]] MetricOwner owner() const noexcept { return owner_; }

    void set(std::int64_t v, MetricOwner by) const noexcept {
        note_(by);
        w_.set(v);
    }
    void add(std::int64_t n, MetricOwner by) const noexcept {
        note_(by);
        w_.add(n);
    }
    /// The writer's own last value (relaxed; the single writer reads back what it stored).
    [[nodiscard]] std::int64_t get() const noexcept { return w_.get(); }

private:
    void note_([[maybe_unused]] MetricOwner by) const noexcept {
#ifndef NDEBUG
        if (writtenBy_ != nullptr) {
            std::atomic_ref<std::uint8_t>(*writtenBy_).fetch_or(metricOwnerBit(by), std::memory_order_relaxed);
        }
        assert(by == owner_ && "a Rune metric row was written by a thread other than its declared MetricOwner");
#endif
    }

    metrics::SlotWriter w_{};
    std::uint8_t* writtenBy_ = nullptr;   // debug owner-tag record (BrokerNode::metricWrittenBy_); may be null
    MetricOwner owner_ = MetricOwner::Control;
};
static_assert(std::is_trivially_copyable_v<RuneMetricSlot>);

}  // namespace rune::node

#endif  // RUNE_NODE_RUNE_METRIC_SLOT_H
