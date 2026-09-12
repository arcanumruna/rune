#pragma once
// NodeRuntimeAgent.h — Composite duty-cycle agent (BR-7a).
//
// Composes the inner consensus agent (Agent& — either ConsensusAgent for
// single-node or NodeConsensusAgent for multi-node) with the ClientGateway
// so that both run on the SAME AgentRunner thread (single-writer invariant).
//
// doWork(nowNs):
//   1. consensusAgent.doWork(nowNs)   — drives Election / IngressRing
//   2. gateway ? gateway->doWork(nowNs) : 0
//   3. return total workCount
//
// Lifecycle: owned by BrokerNode; created in start(), destroyed in stop()
// (BEFORE the AgentRunner is destructed / the gateway is reset).
//
// Single-writer: all accesses to gateway and consensus state happen on the
// single AgentRunner thread. No locking needed; no per-cycle heap.

#include <rune/consensus/ConsensusAgent.h>  // defines rune::consensus::Agent
#include <rune/gateway/ClientGateway.h>

#include <cstdint>

namespace rune::node {

class NodeRuntimeAgent final : public rune::consensus::Agent {
public:
    // @param consensusAgent  Inner agent (ConsensusAgent or NodeConsensusAgent).
    //                        Must outlive this object.
    // @param gateway         ClientGateway pointer (may be null; non-owning).
    //                        Must outlive this object.
    NodeRuntimeAgent(rune::consensus::Agent&    consensusAgent,
                     rune::gateway::ClientGateway* gateway) noexcept
        : consensus_(consensusAgent)
        , gateway_(gateway)
    {}

    NodeRuntimeAgent(const NodeRuntimeAgent&)            = delete;
    NodeRuntimeAgent& operator=(const NodeRuntimeAgent&) = delete;

    // -----------------------------------------------------------------------
    // Agent::doWork — single-writer duty cycle.
    // Returns sum of work done by consensus + gateway.
    // -----------------------------------------------------------------------
    int doWork(int64_t nowNs) override {
        int work = consensus_.doWork(nowNs);
        if (gateway_) {
            work += gateway_->doWork(nowNs);
        }
        return work;
    }

private:
    rune::consensus::Agent&       consensus_;
    rune::gateway::ClientGateway* gateway_;   // non-owning; null-safe
};

} // namespace rune::node
