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
#include <rune/node/LogCleanerAgent.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/ReplicationMetadataBridge.h>
#include <rune/node/GroupCoordinationService.h>
#include <rune/node/TransactionCoordinationService.h>
#include <rune/node/MetadataService.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerEgress.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/AdminChannels.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/replication/AeronPartitionTransport.h>
#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/transaction/ReplicatedTxnStateStore.h>
#include <rune/storage/Log.h>
#include <rune/storage/InMemoryLog.h>

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
#include <functional>
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
// AdminWorkerAgent — wraps AdminWorker + its cross-thread SPSC rings into an
// Agent so it can be driven by an AgentRunner on a dedicated thread.
//
// Shared-nothing (Rule #1): AdminWorkerAgent owns the AdminWorker (which SOLELY
// owns the __topic_schemas replica).  Every cross-thread hop is an SPSC ring
// (Rule #2, Aeron-style):
//
//   requestRing_ (gateway/control → admin) : DefineTopic / GetSchema ops.
//   replyRing_   (control → admin)          : authoritative topicId back.
//   roleRing_    (control → admin)           : Follower/Leader role events.
//   createReqRing_ (admin → control)         : "create this physical topic".
//
// The dead unsynchronized linked-list enqueue path (Task 6) is REMOVED.
// ---------------------------------------------------------------------------
class AdminWorkerAgent final : public rune::consensus::Agent,
                               public rune::gateway::IAdminRequestSink {
public:
    AdminWorkerAgent(SchemaRegistry& reg,
                     rune::replication::PartitionReplica* replica,
                     int32_t selfId,
                     std::vector<int32_t> replicaSet,
                     AdminDeps deps)
        : worker_(reg, replica, selfId, std::move(replicaSet), std::move(deps))
        , requestRing_(kAdminRequestRingBytes)
        , replyRing_(kCreateTopicRingBytes)
        , roleRing_(kAdminRoleRingBytes) {}

    const char* name() const noexcept override { return "rune-admin"; }

    // ── IAdminRequestSink (gateway → admin ring adapter; RULING-2 F) ──────────
    // Called on the gateway/control thread; encodes onto requestRing_ (SPSC).
    bool submitDefineTopic(int64_t sessionId, int64_t correlationId,
                           std::string_view name, uint32_t partitionCount,
                           std::string_view keyField, bool validate,
                           const uint8_t* descriptorBlob,
                           uint32_t descriptorLen) override {
        DefineTopicMsg m;
        m.session        = sessionId;
        m.corr           = correlationId;
        m.partitionCount = static_cast<int32_t>(partitionCount);
        m.validate       = validate;
        m.name.assign(name);
        m.keyField.assign(keyField);
        if (descriptorBlob && descriptorLen)
            m.blob.assign(descriptorBlob, descriptorBlob + descriptorLen);
        return encodeDefineTopic(requestRing_, m);
    }

    bool submitGetSchema(int64_t sessionId, int64_t correlationId,
                         std::string_view name) override {
        GetSchemaMsg m;
        m.session = sessionId;
        m.corr    = correlationId;
        m.name.assign(name);
        return encodeGetSchema(requestRing_, m);
    }

    int doWork(int64_t nowNs) noexcept override {
        int work = 0;

        // 1. Drain role events (control → admin) FIRST so leadership is current
        //    before any DDL runs this tick.
        work += roleRing_.drain([this](int32_t /*tid*/,
                                       aeron::concurrent::AtomicBuffer& b,
                                       aeron::util::index_t off,
                                       aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            worker_.onRole(decodeAdminRole(p, static_cast<std::size_t>(len)));
        }, 16);

        // 2. Drain create-topic replies (control → admin).
        work += replyRing_.drain([this](int32_t /*tid*/,
                                        aeron::concurrent::AtomicBuffer& b,
                                        aeron::util::index_t off,
                                        aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            worker_.onCreateTopicReply(decodeCreateTopicReply(p, static_cast<std::size_t>(len)));
        }, 16);

        // 3. Drain gateway requests (DefineTopic / GetSchema).
        work += requestRing_.drain([this](int32_t tid,
                                          aeron::concurrent::AtomicBuffer& b,
                                          aeron::util::index_t off,
                                          aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            const auto  n = static_cast<std::size_t>(len);
            if (tid == kMsgDefineTopic) {
                worker_.submitDefineTopic(decodeDefineTopic(p, n));
            } else if (tid == kMsgGetSchema) {
                worker_.submitGetSchema(decodeGetSchema(p, n));
            }
        }, 16);

        // 4. Advance the DDL state machine + drive the replica.
        work += worker_.doWork(nowNs);
        return work;
    }

    // -----------------------------------------------------------------------
    // Producer-side accessors — the gateway/control thread enqueues here.
    // requestRing_ producer = gateway/control duty-cycle thread.
    // replyRing_ / roleRing_ producer = control thread.
    // createReqRing_ consumer = control thread; producer = admin (via deps).
    // -----------------------------------------------------------------------
    OneToOneByteRing& requestRing()   noexcept { return requestRing_; }
    OneToOneByteRing& replyRing()     noexcept { return replyRing_;   }
    OneToOneByteRing& roleRing()      noexcept { return roleRing_;    }
    OneToOneByteRing& createReqRing() noexcept { return createReqRing_; }
    OneToOneByteRing& egressRing()    noexcept { return egressRing_;   } // admin → control (response frames)

    AdminWorker& worker() noexcept { return worker_; }

private:
    AdminWorker      worker_;
    OneToOneByteRing requestRing_;     // gateway/control → admin (DefineTopic/GetSchema)
    OneToOneByteRing replyRing_;       // control → admin (createTopic reply)
    OneToOneByteRing roleRing_;        // control → admin (role events)
    OneToOneByteRing createReqRing_{kCreateTopicRingBytes}; // admin → control (createTopic req)
    OneToOneByteRing egressRing_{kAdminRequestRingBytes};   // admin → control (response frames: [sessionId:i64][frame])
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

    /// Return the current leadershipTermId from the election state.
    /// Observer-thread-safe: reads the atomic stateCode to decide the path,
    /// then reads the duty-cycle-written term from the Election.
    /// Used by main.cpp to annotate leadership log messages with the term so
    /// the DR-5 oracle can distinguish stale pre-election announcements from
    /// current-term announcements (term-aware assert_single_leader).
    rune::consensus::Term leadershipTermId() const noexcept;

    /// Publish this node's current role/term/leaderId into the Aeron node-role
    /// counter (RoleCounter.h) in the driver's CnC file.  Idempotent + cheap:
    /// call it from the main poll loop on every tick.  No-op until the driver
    /// has acked the counter allocation (findCounter returns the live Counter).
    /// Observer-thread-safe: reads the same atomic leaderId()/term accessors the
    /// leadership log uses; the counter is the machine-readable signal the
    /// dr-e2e harness reads (via rune_role) instead of scraping logs.
    void publishRole() noexcept;

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

    /// Test-only: return the MetadataService (for assignment() queries in tests).
    /// Non-null after start() if metadata service construction succeeded.
    /// NOT safe for concurrent use — call only when duty cycle is between
    /// doWork calls or when the node is stopped.
    MetadataService* metadataServiceForTest() const noexcept {
        return metadataService_.get();
    }

    /// Test-only: return the SchemaRegistry (for byName/byId queries in tests).
    /// Non-null after start() if schema persistence construction succeeded.
    /// NOT safe for concurrent use — call only when admin thread is quiesced or
    /// the node is stopped.
    SchemaRegistry* schemaRegistryForTest() const noexcept {
        return schemaRegistry_.get();
    }

    /// Test-only: return raw pointer to the __topic_schemas Log (for record-count
    /// assertions in SchemaPersistenceTest).
    rune::storage::Log* schemaLogForTest() const noexcept {
        return schemaLog_.get();
    }

    /// Test-only: return the AdminWorkerAgent (for direct onDefineTopic calls).
    /// Used by SchemaPersistenceTest to drive the AdminWorker without going
    /// through the gateway (Task 8).
    AdminWorkerAgent* adminWorkerAgentForTest() const noexcept {
        return adminWorkerAgent_.get();
    }

    /// Test-only: return the I/O throttle rate wired into the LogCleanerAgent.
    /// -1 = unlimited.  Valid after start().  Used by CP-10 Fix-1 wiring test.
    [[nodiscard]] int64_t cleanerThrottleRateForTest() const noexcept {
        if (!cleanerAgent_) return -2; // -2 = no cleaner (sentinel for test)
        return cleanerAgent_->throttleRateForTest();
    }

    /// Test-only: worker id owning (topicId, partition) per the WorkerPool router.
    /// Returns -1 if workerPool_ is null (no topics configured or E1 not wired).
    /// NOT concurrency-safe; call between doWork calls or when the node is stopped.
    /// Used by WorkerPoolParityTest to assert co-group affinity and N>1 worker usage.
    [[nodiscard]] int32_t workerForPartitionForTest(int32_t topicId,
                                                    int32_t partition) const noexcept {
        return workerPool_ ? workerPool_->ownerWorkerId(topicId, partition) : -1;
    }

    /// read-rework T3/T4 test-only: the per-worker dynamic-MDC egress publications
    /// created by buildWorkerPool (empty unless perWorkerIngressEnabled && aeron_).
    /// Lets a test assert per-worker egress creation + stream-id assignment.
    [[nodiscard]] const std::vector<std::unique_ptr<WorkerEgress>>&
    workerEgressesForTest() const noexcept {
        return workerEgresses_;
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
    void buildWorkerPool();    // E1: shard→worker assignment (must call after replicaRegistry_ is fully built)
    void buildAdminWorker();   // Task 6: __topic_schemas log/replica + AdminWorker thread

    // Task 8b (RULING-4, SINGLE-NODE): create + register the user
    // PartitionReplica(s) for a RUNTIME-defined topic (created via
    // DefineTopic→AdminWorker→control-hook createTopic).  Mirrors the boot
    // buildReplication() user-topic path: reuses the dirs/log created by
    // TopicRegistry::createTopic (Task 5), constructs one PartitionReplica per
    // partition (Follower / epoch 0), registers each in replicaRegistry_ +
    // replTransport_ (which bumps the registry generation → WorkerPool::reconcile
    // assigns it next control cycle), and registers the topic with the
    // MetadataService so its assignment is emitted and the bridge promotes the
    // replica to Leader.  Control-thread only (same thread that owns these
    // registries — no cross-thread mutation).  Returns the number of partitions
    // registered.  Multi-node partition placement is OUT OF SCOPE (deferred).
    int32_t createRuntimeUserReplicas(const TopicSpec& spec);

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
    //
    // memoryLogs_ is declared BEFORE replicaRegistry_ so that in the implicit
    // destructor (members destroyed in reverse declaration order) the replicas
    // are destroyed BEFORE the InMemoryLog instances they point at, preventing
    // dangling-ILog* accesses on the destructor-only teardown path.
    // -----------------------------------------------------------------------
    // InMemoryLog instances for memory-backed user-topic partitions (Task M1).
    // Owned here so their lifetime exceeds replicaRegistry_ (see ordering note).
    std::vector<std::unique_ptr<rune::storage::InMemoryLog>> memoryLogs_;
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

    // -----------------------------------------------------------------------
    // Consumer-group coordination (CG-5 wiring — created in buildGateway()).
    // offsetLog_     — durable Log for __consumer_offsets (one partition).
    // offsetReplica_ — PartitionReplica wrapping the offset Log (single-node
    //                  with InMemoryTransport; role transitions handled by bridge).
    // offsetStore_   — ReplicatedOffsetStore for committed-offset persistence.
    // groupCoordService_ — GroupCoordinationService (dispatches JoinGroup etc.).
    //
    // Destruction order (reverse of construction, in stop()):
    //   runtimeAgent_ → groupCoordService_ → offsetStore_ → offsetReplica_
    //   → offsetLog_ → gateway_ → …
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::storage::Log>                        offsetLog_;
    std::unique_ptr<rune::replication::PartitionReplica>       offsetReplica_;
    std::unique_ptr<rune::coordination::ReplicatedOffsetStore> offsetStore_;
    std::unique_ptr<GroupCoordinationService>                  groupCoordService_;

    // -----------------------------------------------------------------------
    // Transaction coordination (TX-3 wiring — created in buildGateway()).
    // txnLog_          — durable Log for __transaction_state (one partition).
    // txnReplica_      — PartitionReplica wrapping the txn Log (majority-durable).
    // txnState_        — ReplicatedTxnStateStore for transaction metadata.
    // txnCoordService_ — TransactionCoordinationService (dispatches InitProducerId etc.).
    //
    // Destruction order (reverse of construction, in stop()):
    //   runtimeAgent_ → txnCoordService_ → txnState_ → txnReplica_ (in registry)
    //   → txnLog_ → …
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::storage::Log>                             txnLog_;
    std::unique_ptr<rune::replication::PartitionReplica>            txnReplica_;
    std::unique_ptr<rune::transaction::ReplicatedTxnStateStore>     txnState_;
    std::unique_ptr<TransactionCoordinationService>                 txnCoordService_;

    // -----------------------------------------------------------------------
    // Cluster metadata (MD1 wiring — created in buildGateway()).
    // metadataLog_     — durable Log for __cluster_metadata (one partition).
    // metadataReplica_ — PartitionReplica wrapping the metadata Log.
    //                    Registered in ReplicaRegistry so the bridge drives
    //                    its leader/follower role from cluster leadership
    //                    (FINDING-1: DO NOT follow the assignment map).
    // metadataService_ — MetadataService: applies committed records on doWork,
    //                    exposes assignment(topicId, partition).
    //
    // topicId=-3 is the sentinel for __cluster_metadata (offsets=-1, txn=-2).
    //
    // Destruction order (reverse of construction, in stop()):
    //   runtimeAgent_ → metadataService_ → metadataReplica_ (in registry)
    //   → metadataLog_ → …
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::storage::Log>                   metadataLog_;
    std::unique_ptr<rune::replication::PartitionReplica>  metadataReplica_;
    std::unique_ptr<MetadataService>                      metadataService_;

    // -----------------------------------------------------------------------
    // Schema registry (Task 6 — __topic_schemas persistence).
    // schemaLog_         — durable Log for __topic_schemas (one partition).
    // schemaReplica_     — PartitionReplica wrapping the schema Log.
    //                      topicId=-4 (kSchemaTopicId), cleanupPolicy=Compact.
    //                      Majority-durable (minIsr=2) like __transaction_state.
    //                      Registered in replicaRegistry_ so the bridge drives
    //                      its leader/follower role from cluster leadership.
    // schemaRegistry_    — In-memory schema registry (owned by admin thread).
    //                      Rebuilt from schemaLog_ on startup.
    // adminWorkerAgent_  — AdminWorkerAgent running on adminWorkerRunner_ thread.
    //                      Owns the SchemaRegistry; dispatches DefineTopic/Get ops.
    //
    // Destruction order (reverse of construction, in stop()):
    //   adminWorkerRunner_ → adminWorkerAgent_ → schemaRegistry_
    //   → schemaReplica_ (in registry) → schemaLog_ → …
    // -----------------------------------------------------------------------
    std::unique_ptr<rune::storage::Log>                   schemaLog_;
    std::unique_ptr<rune::replication::PartitionReplica>  schemaReplica_;
    std::unique_ptr<SchemaRegistry>                       schemaRegistry_;
    std::unique_ptr<AdminWorkerAgent>                     adminWorkerAgent_;
    // Task 8a: last leadership state posted to the admin role ring (control thread).
    bool                                                  adminLastLeader_ = false;
    int32_t                                               adminLastEpoch_  = -1;
    rune::consensus::BackoffIdleStrategy                  adminWorkerIdleStrategy_;
    rune::consensus::MonotonicNanoClock                   adminWorkerClock_;
    std::unique_ptr<rune::consensus::AgentRunner>         adminWorkerRunner_;

    // -----------------------------------------------------------------------
    // E1: WorkerPool — shard→worker ownership map (built after replicaRegistry_).
    // Destroyed AFTER runtimeAgent_ (which holds a non-owning ptr) and BEFORE
    // replicaRegistry_ (pool holds non-owning replica ptrs; registry owns them).
    // Declaration order enforces this via reverse-destruction in the destructor.
    // -----------------------------------------------------------------------
    std::unique_ptr<WorkerPool> workerPool_;  // E1: shard->worker ownership

    // E5.1.3: per-worker Aeron ingress subscriptions.
    // One WorkerIngress per worker, allocated in buildWorkerPool() when
    // perWorkerIngressEnabled is true.  Each WorkerIngress subscribes to
    // (config_.clientEndpoint.channel, workerIngressBaseStreamId + workerId).
    // Owned here; workers hold non-owning raw pointers (via setWorkerIngress).
    // Empty when perWorkerIngressEnabled is false (default).
    std::vector<std::unique_ptr<WorkerIngress>> workerIngresses_;

    // read-rework T3/T4: per-worker dynamic-MDC egress publications.  One per worker
    // when perWorkerIngressEnabled && aeron_.  Owned here; workers hold non-owning
    // raw pointers (via setWorkerEgress).  Empty otherwise.
    std::vector<std::unique_ptr<WorkerEgress>> workerEgresses_;

    // Composite agent: wraps inner consensus agent + gateway + replication (single-writer).
    std::unique_ptr<NodeRuntimeAgent> runtimeAgent_;

    // -----------------------------------------------------------------------
    // CP-6: LogCleanerAgent — runs on its own AgentRunner thread (off the
    // duty-cycle / append hot path).  Wired after replicaRegistry_ is built
    // in buildGateway().  Stopped BEFORE runtimeAgent_ in stop() to avoid a
    // race where the cleaner enqueues a swap while the duty cycle has stopped
    // draining.  The cleaner's AgentRunner uses a BackoffIdleStrategy (same
    // as the main runner) but on a separate thread so compaction I/O (fsync)
    // never delays appends.
    // -----------------------------------------------------------------------
    std::unique_ptr<LogCleanerAgent>              cleanerAgent_;
    rune::consensus::BackoffIdleStrategy          cleanerIdleStrategy_;
    rune::consensus::MonotonicNanoClock           cleanerClock_;
    std::unique_ptr<rune::consensus::AgentRunner> cleanerRunner_;

    // -----------------------------------------------------------------------
    // Runtime (AgentRunner + clock + idle)
    // -----------------------------------------------------------------------
    rune::consensus::MonotonicNanoClock clock_;
    // Selected from config_.runtimeIdleStrategy (RUNE_RUNTIME_IDLE): Backoff
    // (default) or BusySpin (sub-100µs produce/ack round-trips; pegs the core).
    std::unique_ptr<rune::consensus::IdleStrategy> idleStrategy_;
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
    // Aeron node-role counter (RoleCounter.h) — machine-readable leadership.
    // Allocated in start() after aeron_ connects; updated by publishRole().
    // roleCounterRegId_ is the pending registration id until the driver acks
    // (findCounter returns the live Counter, cached in roleCounter_).
    // -----------------------------------------------------------------------
    std::int64_t                     roleCounterRegId_ = -1;
    std::int64_t                     termCounterRegId_ = -1;
    std::shared_ptr<aeron::Counter>  roleCounter_;
    std::shared_ptr<aeron::Counter>  termCounter_;

    // -----------------------------------------------------------------------
    // State flags
    // -----------------------------------------------------------------------
    std::atomic<bool> started_{false};
    std::atomic<bool> stopped_{false};
};

} // namespace rune::node
