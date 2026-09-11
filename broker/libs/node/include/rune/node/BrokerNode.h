#pragma once
// BrokerNode.h — Broker node consensus runtime (BR-2 + BR-3).
//
// Assembles the full consensus engine from a NodeConfig:
//   NodeStateFile + TermLog (durable, in config.dataDir)
//   1-node: NullSink → SbeConsensusPublisher (BR-2 path, unchanged)
//   N-node: UdpMessageSink (per-peer ExclusivePublications) + AeronIngress +
//           NodeConsensusAgent (composite: poll ingress + doWork) (BR-3 path)
//   ClusterMember vector (built from config.peers)
//   Election (startup values)
//   IngressRing
//   ConsensusAgent (inner duty-cycle agent)
//   MonotonicNanoClock + BackoffIdleStrategy
//   AgentRunner (one thread, no busy-spin)
//   Embedded Aeron media driver + Aeron client
//
// Single-node: self-elects via NullSink, no UDP.
// Multi-node: real per-peer UDP publications + ingress subscription.

#include <rune/node/NodeConfig.h>
#include <rune/node/UdpMessageSink.h>
#include <rune/node/NodeConsensusAgent.h>

#include <rune/consensus/NodeStateFile.h>
#include <rune/consensus/TermLog.h>
#include <rune/consensus/MessageSink.h>
#include <rune/consensus/SbeConsensusPublisher.h>
#include <rune/consensus/ClusterMember.h>
#include <rune/consensus/Election.h>
#include <rune/consensus/IngressRing.h>
#include <rune/consensus/ConsensusAgent.h>
#include <rune/consensus/AeronIngress.h>
#include <rune/consensus/NanoClock.h>
#include <rune/consensus/IdleStrategy.h>
#include <rune/consensus/AgentRunner.h>
#include <rune/consensus/Types.h>

// Aeron embedded driver (C API)
extern "C" {
#include <aeronmd/aeronmd.h>
}
#include <Aeron.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace rune::node {

// ---------------------------------------------------------------------------
// NullSink — no-op outbound transport (sufficient for 1-node; BR-3 replaces)
// ---------------------------------------------------------------------------
class NullSink final : public rune::consensus::MessageSink {
public:
    void send(rune::consensus::NodeId,
              const uint8_t*,
              std::size_t) override {}
};

// ---------------------------------------------------------------------------
// BrokerNode
// ---------------------------------------------------------------------------
class BrokerNode {
public:
    explicit BrokerNode(NodeConfig config);
    ~BrokerNode();

    // Non-copyable, non-movable (owns threads + Aeron state)
    BrokerNode(const BrokerNode&)            = delete;
    BrokerNode& operator=(const BrokerNode&) = delete;

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// Create dataDir if missing; start the embedded driver + Aeron client;
    /// launch the AgentRunner thread.
    void start();

    /// Bounded wait for this node to reach LEADER_READY or CLOSED.
    /// Returns true if leadership reached within timeout, false otherwise.
    bool awaitLeader(std::chrono::milliseconds timeout);

    /// Signal the agent thread to stop, join it, then close the Aeron client
    /// and embedded media driver.  Idempotent.
    void stop();

    // -----------------------------------------------------------------------
    // Observers (thread-safe: atomic reads)
    // -----------------------------------------------------------------------

    bool   isLeader()  const noexcept;
    rune::consensus::NodeId leaderId() const noexcept;

private:
    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------
    void startEmbeddedDriver();
    void stopEmbeddedDriver() noexcept;
    void buildConsensusObjects();
    void buildSingleNodeConsensus();
    void buildMultiNodeConsensus();

    // -----------------------------------------------------------------------
    // Configuration (immutable after construction)
    // -----------------------------------------------------------------------
    NodeConfig config_;

    // -----------------------------------------------------------------------
    // Consensus objects (constructed in start())
    // -----------------------------------------------------------------------

    // Durable persistence (paths under config_.dataDir)
    std::unique_ptr<rune::consensus::NodeStateFile> nodeStateFile_;
    std::unique_ptr<rune::consensus::TermLog>       termLog_;

    // 1-node path: NullSink (BR-2)
    NullSink nullSink_;
    std::unique_ptr<rune::consensus::SbeConsensusPublisher> publisher_;

    // N-node path: UDP sink + ingress (BR-3)
    std::unique_ptr<UdpMessageSink>             udpSink_;
    std::unique_ptr<rune::consensus::AeronIngress> aeronIngress_;
    // Composite agent wrapping ingress poll + ConsensusAgent (multi-node only)
    std::unique_ptr<NodeConsensusAgent>         nodeAgent_;

    // Cluster membership
    std::vector<rune::consensus::ClusterMember> clusterMembers_;

    // Core consensus state machine (constructed last; holds references)
    std::unique_ptr<rune::consensus::IngressRing>    ingressRing_;
    std::unique_ptr<rune::consensus::Election>       election_;
    std::unique_ptr<rune::consensus::ConsensusAgent> agent_;

    // -----------------------------------------------------------------------
    // Runtime (AgentRunner + clock + idle)
    // -----------------------------------------------------------------------
    rune::consensus::MonotonicNanoClock clock_;
    rune::consensus::BackoffIdleStrategy idleStrategy_;
    std::unique_ptr<rune::consensus::AgentRunner> agentRunner_;

    // -----------------------------------------------------------------------
    // Aeron embedded driver + client
    // -----------------------------------------------------------------------
    aeron_driver_context_t* driverCtx_  = nullptr;
    aeron_driver_t*         driver_     = nullptr;
    std::atomic<bool>       driverRunning_{false};
    std::thread             driverThread_;

    std::shared_ptr<aeron::Aeron> aeron_;

    // -----------------------------------------------------------------------
    // State flags
    // -----------------------------------------------------------------------
    std::atomic<bool> started_{false};
    std::atomic<bool> stopped_{false};
};

} // namespace rune::node
