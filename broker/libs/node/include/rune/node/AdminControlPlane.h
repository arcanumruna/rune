#pragma once
// AdminControlPlane.h — the runtime's schema DDL control-plane step (Task 8a
// RULING-2 B), as a concrete type the NodeRuntimeAgent calls DIRECTLY each duty
// cycle.  Replaces a per-cycle std::function hook: under fused placement (spec
// §3.2) the runtime cycle runs inside worker 0's pass, where §1a.5 forbids
// std::function and virtual dispatch.  Defined in BrokerNode.cpp (it services
// BrokerNode-owned rings, the TopicRegistry and the gateway egress).
#include <cstdint>

namespace rune::node {

class BrokerNode;

class AdminControlPlane final {
public:
    explicit AdminControlPlane(BrokerNode& node) noexcept : node_(node) {}
    int doWork(int64_t nowNs);

    /// Task 23 fix round 3 (R2-I1): the largest gap between two consecutive control (runtime) passes (ns) since
    /// the last reset — in fused placement, worker 0's pass that carries the runtime.  Control-thread-owned plain
    /// values, read ON the control thread (BrokerNode::passGapsForTest).  The reset keeps the last pass time.
    [[nodiscard]] int64_t maxPassGapNs() const noexcept { return maxPassGapNs_; }
    void resetMaxPassGapForTest() noexcept { maxPassGapNs_ = 0; }

private:
    BrokerNode& node_;
    int64_t     lastPassNs_ = 0, maxPassGapNs_ = 0;
};

} // namespace rune::node
