#pragma once
// BrokerNode.h — Broker node consensus runtime (BR-2 + BR-3 + BR-7a).
//
// Assembles the full consensus engine from a NodeConfig:
//   NodeStateFile + TermLog (durable, in config.dataDir)
//   1-node: NullSink → SbeConsensusPublisher (BR-2 path, unchanged)
//   N-node: UdpMessageSink (per-peer ExclusivePublications) + AeronIngress +
//           NodeConsensusAgent (composite: poll ingress + doWork) (BR-3 path)
//   ClientGateway (BR-7a): runs inside the duty-cycle via NodeRuntimeAgent.
//   ClusterMember vector (built from config.peers)
//   Election (startup values)
//   IngressRing
//   ConsensusAgent (inner duty-cycle agent)
//   NodeRuntimeAgent (wraps inner consensus agent + gateway, drives both)
//   MonotonicNanoClock + BackoffIdleStrategy
//   AgentRunner (one thread, no busy-spin)
//   Embedded Aeron media driver + Aeron client
//
// Single-node: self-elects via NullSink, no UDP.
// Multi-node: real per-peer UDP publications + ingress subscription.
// Both paths now serve clients via ClientGateway (single-writer duty-cycle).

#include <rune/node/NodeConfig.h>
#include <rune/node/UdpMessageSink.h>
#include <rune/node/NodeConsensusAgent.h>
#include <rune/node/NodeRuntimeAgent.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/replication/AeronPartitionTransport.h>

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

    /// Return the leader's client Aeron channel for data-op redirect (GB-4).
    /// Observer-thread-safe: reads atomic leaderId() + immutable peer config.
    /// - If this node IS the leader → returns empty string (no self-redirect).
    /// - If leader id is known and that peer has a clientPort → returns its
    ///   clientChannel() ("aeron:udp?endpoint=host:port").
    /// - Otherwise (unknown leader, or leader peer has no clientPort) → empty.
    std::string leaderClientEndpoint() const noexcept;

    // -----------------------------------------------------------------------
    // Test-only accessors (GR-2/GR-3 integration tests).
    // NOTE: these reach into duty-cycle-owned state; call only when the node
    // is stopped OR from a synchronized test context that knows the duty cycle
    // is between doWork calls.  NOT safe for general concurrent use.
    // -----------------------------------------------------------------------

    /// Return the PartitionReplica for (topicId, partition), or nullptr.
    rune::replication::PartitionReplica*
    replicaForTest(int32_t topicId, int32_t partition) const noexcept {
        if (!replicaRegistry_) return nullptr;
        return replicaRegistry_->lookup(topicId, partition);
    }

    /// Return the TopicRegistry (for direct Log* access in tests).
    rune::gateway::TopicRegistry*
    topicRegistryForTest() const noexcept {
        return topicRegistry_.get();
    }

    /// Observer-thread-safe: true if the replication metadata bridge has
    /// propagated this node's leadership to all its PartitionReplicas.
    /// Uses the atomic mirror in ReplicationMetadataBridge.
    bool replicationIsLeaderForTest() const noexcept {
        return replBridge_ && replBridge_->isLeaderAtomic();
    }

    /// Observer-thread-safe: the last epoch applied by the replication bridge.
    /// Uses the atomic mirror in ReplicationMetadataBridge.
    rune::replication::Epoch replicationEpochForTest() const noexcept {
        if (!replBridge_) return 0;
        return replBridge_->lastEpochAtomic();
    }

    /// Set the minimum ISR size required for acks=All produces on this node's gateway.
    /// Thread-safe (gateway::ClientGateway::setMinIsr uses std::atomic<int>).
    /// Typically called from tests to configure min-ISR before or after start().
    void setGatewayMinIsr(int minIsr) noexcept {
        if (gateway_) gateway_->setMinIsr(minIsr);
    }

    /// Return this node's ClientGateway (or nullptr).  Test-only: reaches into
    /// duty-cycle-owned state (e.g. pendingAckCount()); call only from a
    /// synchronized test context that knows the duty cycle is between doWork
    /// calls (or with the node stopped).  Used by GR-9 to observe the
    /// deferred-ack queue depth for the acks=All HW-gate boundary test.
    rune::gateway::ClientGateway* gatewayForTest() const noexcept {
        return gateway_.get();
    }

private:
    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------
    void startEmbeddedDriver();
    void stopEmbeddedDriver() noexcept;
    void buildConsensusObjects();
    void buildSingleNodeConsensus();
    void buildMultiNodeConsensus();
    void buildReplication();   // GR-2/GR-3: replicas + transport + bridge
    void buildGateway();

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
    // Topic registry (GB-1) — per-partition Logs, bootstrapped from RUNE_TOPICS.
    // Constructed in start() after dataDir creation; outlives the gateway.
    // Logs are single-writer: accessed only on the AgentRunner duty-cycle thread.
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::gateway::TopicRegistry> topicRegistry_;

    // -----------------------------------------------------------------------
    // Replication (GR-2/GR-3) — per-partition PartitionReplica + transport.
    // Constructed in buildGateway() after aeron_ and topicRegistry_ are live.
    // All replication state is single-writer (AgentRunner duty-cycle thread).
    //
    // Destruction order in stop() (reverse of construction):
    //   runtimeAgent_ → gateway_ → replicaRegistry_ → replTransport_
    //   → replBridge_ → topicRegistry_
    // -----------------------------------------------------------------------
    std::unique_ptr<ReplicaRegistry>                        replicaRegistry_;
    std::unique_ptr<rune::replication::AeronPartitionTransport> replTransport_;
    std::unique_ptr<ReplicationMetadataBridge>              replBridge_;
    // Per-replica no-op InMemoryTransport instances used when replTransport_ is null.
    // Prevents PartitionReplica assert(transport!=nullptr) in single-node / no-repl-port nodes.
    std::vector<std::unique_ptr<rune::replication::InMemoryTransport>> nullTransports_;

    // -----------------------------------------------------------------------
    // Client gateway (BR-7a) — runs inside the duty-cycle via NodeRuntimeAgent.
    // Constructed after aeron_ in start(); destroyed in stop() before aeron_.
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::gateway::ClientGateway> gateway_;

    // Composite agent: wraps inner consensus agent + gateway + replication (single-writer).
    std::unique_ptr<NodeRuntimeAgent> runtimeAgent_;

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
