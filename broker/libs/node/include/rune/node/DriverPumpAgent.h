#pragma once
// DriverPumpAgent.h — interim pumper for an invoker driver + client (fused
// runtime startup, Task 11).  Separate header so EmbeddedDriver.h stays light.
#include <rune/consensus/ConsensusAgent.h>
#include <rune/node/EmbeddedDriver.h>

#include <Aeron.h>

namespace rune::node {

/// Interim pumper for an Invoker driver + its invoker client, run by its own
/// AgentRunner (Aeron: a composite of the driver and client-conductor invokers).
/// Fused runtime (spec §3.2): the runtime's driver + client exist from step 3 of
/// BrokerNode::start(), long before worker 0's loop runs; this agent keeps them
/// serviced through every startup phase, however long.  Hand-off: the runner is
/// stopped and JOINED, then the next owner binds itself (bindPumpThread) at its
/// first pass — never two pumpers.  Startup resolves on that client use the
/// default (sleep-poll) AeronPump while this agent runs.
class DriverPumpAgent final : public rune::consensus::Agent {
public:
    DriverPumpAgent(EmbeddedDriver& driver, aeron::Aeron& client) noexcept
        : driver_(driver), client_(client) {}
    const char* name() const noexcept override { return "rune-rt-pump"; }
    int doWork(int64_t /*nowNs*/) noexcept override {
        if (!bound_) [[unlikely]] {
            driver_.bindPumpThread();          // hand-off: constructing thread → this runner
            bound_ = true;
        }
        return driver_.doWork() + client_.conductorAgentInvoker().invoke();
    }

private:
    EmbeddedDriver& driver_;
    aeron::Aeron&   client_;
    bool            bound_ = false;
};

} // namespace rune::node
