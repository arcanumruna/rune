#pragma once
// ShardAgent.h — the self-contained worker shard duty cycle (spec §3.1).  One
// thread (pinned, busy-spin when pinned) per worker; each pass, in order:
//   1. own embedded driver (INVOKER: conductor + sender + receiver)
//   2. own Aeron client conductor (useConductorAgentInvoker)
//   3-6. Worker::doWork in shard order (intake, replication, acks, io reap)
// Task 11 appends the fused runtime (its own driver + conductor + NodeRuntimeAgent).
//
// Slow-pass alarm (spec §8): the duration of the WORK part of one pass —
// pass start (the runner's injected nowNs) to pass end (the same injected clock
// sampled once after the work).  Idle parks between passes are never counted.
// The clock is a template parameter so the production end-sample is a direct
// (final-class, de-virtualised) call — no vtable dispatch on the duty cycle.
#include <rune/consensus/ConsensusAgent.h>
#include <rune/consensus/NanoClock.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/Worker.h>
#include <rune_fr/FlightRecorder.h>

#include <Aeron.h>

#include <cstdint>

namespace rune::node {

template <class Clock>
class BasicShardAgent final : public rune::consensus::Agent {
public:
    /// A pass longer than this (the whole p99 budget) is an alarm (spec §8).
    static constexpr int64_t kSlowPassNs = 100'000;

    BasicShardAgent(Worker& worker, EmbeddedDriver& driver, aeron::Aeron& client, const Clock& clock) noexcept
        : worker_(worker), driver_(driver), client_(client), clock_(clock) {}

    const char* name() const noexcept override { return worker_.name(); }

    int doWork(int64_t nowNs) override {
        RUNE_FR_SPAN_RUNTIME(rune_fr::Site::ShardPass, 0);
        if (!bound_) [[unlikely]] {
            driver_.bindPumpThread();          // ownership hand-off: startup thread → this thread
            bound_ = true;
        }
        int work = driver_.doWork();                        // 1. own driver duty cycle
        work += client_.conductorAgentInvoker().invoke();   // 2. own client conductor
        work += worker_.doWork(nowNs);                      // 3-6. Worker (final → direct call)
        if (clock_.nowNs() - nowNs > kSlowPassNs) [[unlikely]] ++slowPasses_;
        return work;
    }

    [[nodiscard]] uint64_t slowPassesForTest() const noexcept { return slowPasses_; }

private:
    Worker&         worker_;
    EmbeddedDriver& driver_;
    aeron::Aeron&   client_;
    const Clock&    clock_;
    bool            bound_      = false;
    uint64_t        slowPasses_ = 0;
};

/// Production shard agent: the node's monotonic clock (final class).
using ShardAgent = BasicShardAgent<rune::consensus::MonotonicNanoClock>;
extern template class BasicShardAgent<rune::consensus::MonotonicNanoClock>;

} // namespace rune::node
