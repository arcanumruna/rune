#pragma once
// ShardAgent.h — the self-contained worker shard duty cycle (spec §3.1).  One
// thread (pinned; backoff idle unless RUNE_WORKER_IDLE=busyspin) per worker; each pass, in order:
//   1. own embedded driver (INVOKER: conductor + sender + receiver)
//   2. own Aeron client conductor (useConductorAgentInvoker)
//   3-6. Worker::doWork in shard order (intake, replication, acks, io reap)
//   7. fused runtime placement only (spec §3.2, worker 0): the runtime's own
//      invoker driver → its client conductor → NodeRuntimeAgent::doWork.
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

#include <algorithm>
#include <cstdint>

namespace rune::node {

class NodeRuntimeAgent;

namespace detail {
/// One fused-runtime duty cycle (spec §3.2): runtime driver → runtime client
/// conductor → NodeRuntimeAgent::doWork (a final class → direct call).  Out of
/// line because NodeRuntimeAgent.h includes this header (via WorkerPool.h).
int fusedRuntimePass(NodeRuntimeAgent& runtime, EmbeddedDriver& driver, aeron::Aeron& client,
                     int64_t nowNs) noexcept;
/// A4: the fused runtime when it rides the shard's own driver + client (the pass
/// already ran that driver and conductor): only the runtime agent's work.
int fusedRuntimeAgentOnly(NodeRuntimeAgent& runtime, int64_t nowNs) noexcept;
} // namespace detail

template <class Clock>
class BasicShardAgent final : public rune::consensus::Agent {
public:
    /// A pass longer than this (the whole p99 budget) is an alarm (spec §8).
    static constexpr int64_t kSlowPassNs = 100'000;

    BasicShardAgent(Worker& worker, EmbeddedDriver& driver, aeron::Aeron& client, const Clock& clock) noexcept
        : worker_(worker), driver_(driver), client_(client), clock_(clock),
          drainBytes_(drainBytesFor(driver.maxMessagesPerSend(), driver.mtuLength())) {}

    /// A8: repeat the driver duty within the pass?  Aeron's duty work is bytes sent +
    /// received + commands.  With the receive-full count (rxFull* >= 0): repeat when
    /// the duty moved at least one full send batch (`drainBytes`) or a receive call
    /// filled every slot.  Without it (-1: no rune_gso bindings): the A1 rule, repeat
    /// while the duty did any work.
    [[nodiscard]] static constexpr bool repeatDuty(int w, int drainBytes, int64_t rxFullBefore,
                                                   int64_t rxFullAfter) noexcept {
        if (rxFullAfter < 0) return w != 0;
        return w >= drainBytes || rxFullAfter > rxFullBefore;
    }

    /// A8: the duty-repeat threshold — a full publication send batch: maxMessagesPerSend
    /// datagrams, each at least half an MTU once frames are queued (Aeron packs frames
    /// up to the MTU; a datagram under half an MTU means the queue ran dry).
    [[nodiscard]] static constexpr int drainBytesFor(uint32_t maxMessagesPerSend, uint32_t mtu) noexcept {
        return static_cast<int>(std::max<uint32_t>(1u, maxMessagesPerSend) * std::max<uint32_t>(2u, mtu) / 2u);
    }

    const char* name() const noexcept override { return worker_.name(); }

    /// Spec §3.2 fused placement: run the runtime (its own invoker driver + client
    /// conductor + NodeRuntimeAgent) at the end of this shard's pass.  Worker 0
    /// only; set before the runner starts.  Pump ownership of the runtime driver
    /// passes to this thread at its first pass, like the shard's own driver.
    void setFusedRuntime(NodeRuntimeAgent* runtime, EmbeddedDriver* runtimeDriver,
                         aeron::Aeron* runtimeClient) noexcept {
        runtime_ = runtime; runtimeDriver_ = runtimeDriver; runtimeClient_ = runtimeClient;
        sharedDriver_ = runtimeDriver == &driver_ && runtimeClient == &client_;
    }

    int doWork(int64_t nowNs) override {
        RUNE_FR_SPAN_RUNTIME(rune_fr::Site::ShardPass, 0);
        if (!bound_) [[unlikely]] {
            driver_.bindPumpThread();          // ownership hand-off: startup thread → this thread
            if (runtimeDriver_) runtimeDriver_->bindPumpThread();
            bound_ = true;
        }
        // 1. own driver duty cycle — A1: drained while it moves a backlog (bounded),
        //    so one pass moves more than ~1 datagram per stream (invoker driver:
        //    one duty cycle sends/receives only a small batch per transport).
        //    A8: repeated only while a backlog may be left (repeatDuty): a send
        //    batch was full, or a receive filled every recvmmsg slot.  Otherwise
        //    the duty drained what was there and the confirming second duty (a
        //    recvmmsg per transport + a publication walk) is skipped.
        int work = 0;
        int64_t rxFull = driver_.rxFullCount();
        for (int dc = 0; dc < kDriverDutyPerPass; ++dc) {
            const int w = driver_.doWork();
            ++driverDutyCycles_;
            work += w;
            const int64_t rxFullNow = driver_.rxFullCount();
            if (!repeatDuty(w, drainBytes_, rxFull, rxFullNow)) break;
            rxFull = rxFullNow;
        }
        work += client_.conductorAgentInvoker().invoke();   // 2. own client conductor
        work += worker_.doWork(nowNs);                      // 3-6. Worker (final → direct call)
        if (runtime_)                                       // 7. fused runtime (worker 0)
            work += sharedDriver_ ? detail::fusedRuntimeAgentOnly(*runtime_, nowNs)  // A4: rides THIS pass's driver + conductor
                                  : detail::fusedRuntimePass(*runtime_, *runtimeDriver_, *runtimeClient_, nowNs);
        if (clock_.nowNs() - nowNs > kSlowPassNs) [[unlikely]] ++slowPasses_;
        return work;
    }

    [[nodiscard]] uint64_t slowPassesForTest() const noexcept { return slowPasses_; }
    /// A1: most driver duty cycles one pass runs while the driver keeps reporting work.
    static constexpr int kDriverDutyPerPass = 16;
    [[nodiscard]] uint64_t driverDutyCyclesForTest() const noexcept { return driverDutyCycles_; }
    /// A8: a duty that moved at least this many bytes repeats (drainBytesFor).
    [[nodiscard]] int drainBytesForTest() const noexcept { return drainBytes_; }

private:
    Worker&         worker_;
    EmbeddedDriver& driver_;
    aeron::Aeron&   client_;
    const Clock&    clock_;
    int             drainBytes_;   ///< A8: duty-repeat threshold (bytes moved by one duty)
    uint64_t driverDutyCycles_ = 0;
    bool     sharedDriver_     = false;   ///< A4: fused runtime rides this shard's driver + client
    NodeRuntimeAgent* runtime_       = nullptr;   // fused placement only (worker 0)
    EmbeddedDriver*   runtimeDriver_ = nullptr;
    aeron::Aeron*     runtimeClient_ = nullptr;
    bool            bound_      = false;
    uint64_t        slowPasses_ = 0;
};

/// Production shard agent: the node's monotonic clock (final class).
using ShardAgent = BasicShardAgent<rune::consensus::MonotonicNanoClock>;
extern template class BasicShardAgent<rune::consensus::MonotonicNanoClock>;

} // namespace rune::node
