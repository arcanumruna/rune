#pragma once
// ShardAgent.h — the self-contained worker shard duty cycle (spec §3.1).  One
// thread (pinned, busy-spin when pinned) per worker; each pass, in order:
//   1. own embedded driver (INVOKER: conductor + sender + receiver)
//   2. own Aeron client conductor (useConductorAgentInvoker)
//   3-6. Worker::doWork in shard order (intake, replication, acks, io reap)
// Task 11 appends the fused runtime (its own driver + conductor + NodeRuntimeAgent).
#include <rune/consensus/ConsensusAgent.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/Worker.h>

#include <Aeron.h>

#include <cstdint>

namespace rune::node {

class ShardAgent final : public rune::consensus::Agent {
public:
    /// A pass longer than this (the whole p99 budget) is an alarm (spec §8).
    static constexpr int64_t kSlowPassNs = 100'000;

    ShardAgent(Worker& worker, EmbeddedDriver& driver, aeron::Aeron& client) noexcept
        : worker_(worker), driver_(driver), client_(client) {}

    const char* name() const noexcept override { return worker_.name(); }
    int doWork(int64_t nowNs) override;

    [[nodiscard]] uint64_t slowPassesForTest() const noexcept { return slowPasses_; }

private:
    Worker&         worker_;
    EmbeddedDriver& driver_;
    aeron::Aeron&   client_;
    bool            bound_      = false;
    int64_t         lastNowNs_  = 0;
    uint64_t        slowPasses_ = 0;
};

} // namespace rune::node
