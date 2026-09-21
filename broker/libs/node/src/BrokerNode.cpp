#include <rune/node/BrokerNode.h>
#include <rune/node/RoleCounter.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// CG-6 wiring: storage + replication headers for __consumer_offsets.
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
// TX-3 wiring: transaction coordination service + replicated state store.
#include <rune/node/TransactionCoordinationService.h>
#include <rune/transaction/ReplicatedTxnStateStore.h>

namespace fs = std::filesystem;
using namespace rune::consensus;

namespace rune::node {

// ---------------------------------------------------------------------------
// Timing constants — used ONLY for single-node (BR-2) path where NodeConfig
// fields are not yet wired. Multi-node path reads from config_.
// ---------------------------------------------------------------------------
static constexpr int kIngressCapacity = 64 * 1024; //  64 KiB

// ---------------------------------------------------------------------------
// BrokerNode::BrokerNode
// ---------------------------------------------------------------------------
BrokerNode::BrokerNode(NodeConfig config)
    : config_{std::move(config)}
{}

// ---------------------------------------------------------------------------
// BrokerNode::~BrokerNode
// ---------------------------------------------------------------------------
BrokerNode::~BrokerNode() {
    stop();
}

// ---------------------------------------------------------------------------
// BrokerNode::start
// ---------------------------------------------------------------------------
void BrokerNode::start() {
    bool expected = false;
    if (!started_.compare_exchange_strong(expected, true,
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return; // idempotent
    }

    using sclock = std::chrono::steady_clock;
    const auto t0 = sclock::now();

    // 1. Create dataDir if missing.
    fs::path dataDir{config_.dataDir};
    std::error_code ec;
    fs::create_directories(dataDir, ec);
    if (ec) {
        throw std::runtime_error("BrokerNode: cannot create dataDir '" +
                                 config_.dataDir + "': " + ec.message());
    }

    // 2. Build the TopicRegistry (after dataDir creation, before the gateway).
    //    recover() is called for each Log in the ctor so restart resumes at LEO.
    //    Logs are single-writer: only the AgentRunner duty-cycle thread appends/reads.
    topicRegistry_ = std::make_unique<rune::gateway::TopicRegistry>(
        config_.topics, config_.dataDir);

    // 3. Start embedded Aeron media driver.
    startEmbeddedDriver();
    const auto t1 = sclock::now();

    // 4. Connect Aeron client — must point at the same dir as the driver.
    {
        const std::string aeronDir =
            (fs::path{config_.dataDir} / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        if (!aeron_) {
            throw std::runtime_error("BrokerNode: failed to connect Aeron client");
        }

        // Allocate the Aeron node-role counter (RoleCounter.h) in the driver's
        // CnC file.  This is the machine-readable leadership signal (mirrors
        // Aeron Cluster's node-role counter); the dr-e2e harness reads it via
        // rune_role instead of scraping logs.  addCounter returns a pending
        // registration id; the driver acks asynchronously, after which
        // publishRole() resolves it to the live Counter via findCounter().
        std::uint8_t roleKey[kRoleKeyLength];
        encodeRoleKey(roleKey, config_.nodeId);
        roleCounterRegId_ = aeron_->addCounter(
            kRoleCounterTypeId, roleKey, kRoleKeyLength,
            roleCounterLabel(config_.nodeId));
        termCounterRegId_ = aeron_->addCounter(
            kTermCounterTypeId, roleKey, kRoleKeyLength,
            termCounterLabel(config_.nodeId));
    }
    const auto t2 = sclock::now();

    // 5. Build all consensus objects (NodeStateFile, TermLog, Election, …).
    buildConsensusObjects();
    const auto t3 = sclock::now();

    // 6. Build replication (GR-2/GR-3): PartitionReplicas + AeronPartitionTransport
    //    + ReplicationMetadataBridge.  Requires: topicRegistry_ + aeron_ + consensus objs.
    buildReplication();

    // 7. Build the ClientGateway and NodeRuntimeAgent (after aeron_ is live).
    buildGateway();

    // 7. Start the agent runner thread (drives NodeRuntimeAgent, which drives
    //    consensus + gateway on the single-writer thread).
    agentRunner_->start();

    const auto t_start = sclock::now();
    spdlog::info(
        "BrokerNode: node {} started (dataDir={}) "
        "| driver_start={:.1f}ms aeron_connect={:.1f}ms consensus_build={:.1f}ms total={:.1f}ms",
        config_.nodeId, config_.dataDir,
        std::chrono::duration<double, std::milli>(t1 - t0).count(),
        std::chrono::duration<double, std::milli>(t2 - t1).count(),
        std::chrono::duration<double, std::milli>(t3 - t2).count(),
        std::chrono::duration<double, std::milli>(t_start - t0).count());
}

// ---------------------------------------------------------------------------
// BrokerNode::buildConsensusObjects — dispatches to single/multi-node path
// ---------------------------------------------------------------------------
void BrokerNode::buildConsensusObjects() {
    const fs::path dataDir{config_.dataDir};

    // Durable persistence files.
    nodeStateFile_ = std::make_unique<NodeStateFile>(
        (dataDir / "node-state.dat").string());
    termLog_ = std::make_unique<TermLog>(
        (dataDir / "term.log").string());

    // Cluster members — one entry per peer in config_.peers.
    clusterMembers_.clear();
    clusterMembers_.reserve(config_.peers.size());
    for (const auto& peer : config_.peers) {
        ClusterMember m;
        m.id                = peer.id;
        m.logPosition       = 0;
        m.logLeadershipTermId = 0;
        m.leadershipTermId  = 0;
        clusterMembers_.push_back(m);
    }

    // IngressRing (MPSC; 64 KiB payload).
    ingressRing_ = std::make_unique<IngressRing>(kIngressCapacity);

    if (config_.isSingleNode()) {
        buildSingleNodeConsensus();
    } else {
        buildMultiNodeConsensus();
    }
}

// ---------------------------------------------------------------------------
// BrokerNode::buildSingleNodeConsensus — BR-2 path (NullSink, self-elect)
// ---------------------------------------------------------------------------
void BrokerNode::buildSingleNodeConsensus() {
    // Publisher (outbound transport: NullSink — no peers).
    publisher_ = std::make_unique<SbeConsensusPublisher>(nullSink_);

    // Election — use configurable timing from NodeConfig.
    const int64_t electionTimeoutNs =
        config_.electionTimeoutMs * 1'000'000LL;
    const int64_t statusIntervalNs =
        config_.electionStatusIntervalMs * 1'000'000LL;
    const int64_t canvassTimeoutNs =
        config_.startupCanvassTimeoutMs * 1'000'000LL;

    election_ = std::make_unique<Election>(
        /*isNodeStartup=*/true,
        /*gracefulClosedLeaderId=*/-1,
        /*leadershipTermId=*/0,
        /*logLeadershipTermId=*/0,
        /*logPosition=*/0,
        /*appendPosition=*/0,
        clusterMembers_,
        static_cast<NodeId>(config_.nodeId),
        *publisher_,
        *nodeStateFile_,
        *termLog_,
        electionTimeoutNs,
        statusIntervalNs,
        canvassTimeoutNs,
        /*rngSeed=*/42u);

    // ConsensusAgent (plain — no ingress polling needed for 1-node).
    agent_ = std::make_unique<ConsensusAgent>(*election_, *ingressRing_);

    // NOTE: AgentRunner is created in buildGateway() after the gateway is built,
    // so that NodeRuntimeAgent (consensus + gateway) is driven as a unit.
}

// ---------------------------------------------------------------------------
// BrokerNode::buildMultiNodeConsensus — BR-3 path (UDP per-peer + ingress)
// ---------------------------------------------------------------------------
void BrokerNode::buildMultiNodeConsensus() {
    using sclock = std::chrono::steady_clock;
    const NodeId selfId = static_cast<NodeId>(config_.nodeId);

    // -----------------------------------------------------------------------
    // Step A: Create AeronIngress FIRST (subscribe on self's consensus endpoint).
    // This ensures the subscription is active before peer publications connect,
    // minimizing dropped canvass frames and SETUP-handshake latency.
    // -----------------------------------------------------------------------
    const auto& selfPeer = config_.self();
    const auto t_ingress_start = sclock::now();
    aeronIngress_ = std::make_unique<AeronIngress>(
        aeron_,
        selfPeer.consensusChannel(),
        config_.consensusStreamId,
        *ingressRing_);
    const auto t_ingress_end = sclock::now();
    spdlog::debug(
        "BrokerNode[{}]: AeronIngress resolved in {:.1f}ms",
        config_.nodeId,
        std::chrono::duration<double, std::milli>(t_ingress_end - t_ingress_start).count());

    // -----------------------------------------------------------------------
    // Step B: Build per-peer UDP sink (publish to each OTHER peer's endpoint).
    // Subscriptions are now live — publication SETUP handshakes can succeed sooner.
    // -----------------------------------------------------------------------
    std::unordered_map<NodeId, UdpMessageSink::PeerEndpoint> peerEndpoints;
    for (const auto& peer : config_.peers) {
        if (peer.id == config_.nodeId) continue; // skip self
        peerEndpoints[static_cast<NodeId>(peer.id)] =
            UdpMessageSink::PeerEndpoint{
                peer.consensusChannel(),
                config_.consensusStreamId
            };
    }
    const auto t_sink_start = sclock::now();
    udpSink_ = std::make_unique<UdpMessageSink>(aeron_, peerEndpoints);
    const auto t_sink_end = sclock::now();
    spdlog::debug(
        "BrokerNode[{}]: UdpMessageSink publications resolved in {:.1f}ms",
        config_.nodeId,
        std::chrono::duration<double, std::milli>(t_sink_end - t_sink_start).count());

    // Publisher over the UDP sink.
    publisher_ = std::make_unique<SbeConsensusPublisher>(*udpSink_);

    // -----------------------------------------------------------------------
    // Step C: Election with configurable timing from NodeConfig.
    // -----------------------------------------------------------------------
    const int64_t electionTimeoutNs =
        config_.electionTimeoutMs * 1'000'000LL;
    const int64_t statusIntervalNs =
        config_.electionStatusIntervalMs * 1'000'000LL;
    const int64_t canvassTimeoutNs =
        config_.startupCanvassTimeoutMs * 1'000'000LL;

    election_ = std::make_unique<Election>(
        /*isNodeStartup=*/true,
        /*gracefulClosedLeaderId=*/-1,
        /*leadershipTermId=*/0,
        /*logLeadershipTermId=*/0,
        /*logPosition=*/0,
        /*appendPosition=*/0,
        clusterMembers_,
        selfId,
        *publisher_,
        *nodeStateFile_,
        *termLog_,
        electionTimeoutNs,
        statusIntervalNs,
        canvassTimeoutNs,
        /*rngSeed=*/static_cast<uint64_t>(42u + selfId));

    // Inner ConsensusAgent (drives Election, drains IngressRing).
    agent_ = std::make_unique<ConsensusAgent>(*election_, *ingressRing_);

    // Composite NodeConsensusAgent (polls ingress THEN runs ConsensusAgent).
    nodeAgent_ = std::make_unique<NodeConsensusAgent>(
        *aeronIngress_, *agent_, *election_, selfId);

    // NOTE: AgentRunner is created in buildGateway() after the gateway is built,
    // so that NodeRuntimeAgent (nodeAgent + gateway) is driven as a unit.
}

// ---------------------------------------------------------------------------
// BrokerNode::buildReplication — GR-2/GR-3.
//
// Constructs one PartitionReplica per owned partition (sharing the Log* from
// TopicRegistry), one AeronPartitionTransport (per node), and the
// ReplicationMetadataBridge that drives role transitions from consensus.
//
// Prerequisites:
//   - topicRegistry_ is fully built (all Logs recovered).
//   - aeron_ is connected.
//   - config_.peers is populated.
//
// For the DR scope: the cluster leader is the leader of ALL partitions.
// Replication channel per peer: peer.replicationChannel().  If no peer has
// a replication port configured (all zero), we skip transport construction
// but still build replicas with a null transport (no-op for single-node).
//
// Replica initial role: always starts as Follower (epoch 0, leaderId=-1) to
// avoid stale leader assumptions at startup.  The ReplicationMetadataBridge
// will call becomeLeader/becomeFollower on the first cycle after the
// consensus layer resolves the initial leader.
// ---------------------------------------------------------------------------
void BrokerNode::buildReplication() {
    using namespace rune::replication;

    const NodeId selfId = static_cast<NodeId>(config_.nodeId);

    // Collect all NodeIds in the cluster (used as the replica set + ISR seed).
    std::vector<NodeId> allNodeIds;
    allNodeIds.reserve(config_.peers.size());
    for (const auto& peer : config_.peers) {
        allNodeIds.push_back(static_cast<NodeId>(peer.id));
    }

    // -----------------------------------------------------------------------
    // Build replTransport_ — AeronPartitionTransport (requires aeron_).
    // Only constructed when this node has a replication port configured.
    // Built FIRST so we have the transport pointer ready for replica construction.
    // -----------------------------------------------------------------------
    const auto& selfPeer = config_.self();
    const bool hasReplPort = (selfPeer.replicationPort != 0);

    if (hasReplPort && aeron_) {
        const std::string ingressChannel = selfPeer.replicationChannel();
        const int32_t     ingressStream  = config_.replicationStreamId;

        try {
            replTransport_ = std::make_unique<rune::replication::AeronPartitionTransport>(
                aeron_, ingressChannel, ingressStream, selfId);

            // Add each peer (excluding self) as an egress destination.
            for (const auto& peer : config_.peers) {
                if (peer.id == config_.nodeId) continue;       // skip self
                if (peer.replicationPort == 0) continue;       // no replication endpoint
                replTransport_->addPeer(
                    static_cast<NodeId>(peer.id),
                    peer.replicationChannel(),
                    ingressStream);
            }

            spdlog::info("BrokerNode[{}]: AeronPartitionTransport built on {} (stream={}) with {} peers",
                         config_.nodeId, ingressChannel, ingressStream,
                         replTransport_->peerCount());

        } catch (const std::exception& ex) {
            spdlog::warn("BrokerNode[{}]: AeronPartitionTransport construction failed: {}; "
                         "replication transport disabled",
                         config_.nodeId, ex.what());
            replTransport_.reset();
        }
    } else {
        const int32_t topicCountForWarn = topicRegistry_->topicCount();
        if (topicCountForWarn > 0 && config_.peers.size() > 1) {
            spdlog::warn("BrokerNode[{}]: no replicationPort configured — "
                         "replication transport NOT started (topics won't replicate)",
                         config_.nodeId);
        }
    }

    // -----------------------------------------------------------------------
    // Build replicaRegistry_ — one PartitionReplica per (topicId, partition).
    // Each replica shares the Log* from topicRegistry_ (non-owning).
    // Initial role: Follower / epoch 0 (bridge will set correct role on first cycle).
    // Transport pointer: use replTransport_ if available, else use a no-op
    // InMemoryTransport owned locally (no peers = all frames dropped).
    //
    // We store an InMemoryTransport per-node as a fallback when no Aeron transport
    // is available.  This avoids the PartitionReplica assert(transport != nullptr).
    // -----------------------------------------------------------------------
    replicaRegistry_ = std::make_unique<ReplicaRegistry>();

    const int32_t topicCount = topicRegistry_->topicCount();
    for (int32_t topicId = 0; topicId < topicCount; ++topicId) {
        const auto* spec = topicRegistry_->topic(topicId);
        if (!spec) continue;

        for (int32_t part = 0; part < spec->partitionCount; ++part) {
            rune::storage::Log* logPtr = topicRegistry_->log(topicId, part);
            if (!logPtr) {
                spdlog::warn("BrokerNode[{}]: log not found for topic {} partition {}; skipping replica",
                             config_.nodeId, spec->name, part);
                continue;
            }

            PartitionConfig pcfg;
            pcfg.topicId              = topicId;   // DR-BUG-5: multi-topic routing
            pcfg.partitionId          = static_cast<PartitionId>(part);
            pcfg.selfId               = selfId;
            pcfg.replicaLagMaxMs      = config_.replicaLagMaxMs;  // GR-5: configurable
            pcfg.replicaLagMaxOffset  = 10'000;
            pcfg.minIsr               = config_.gatewayMinIsr;    // M1: consistent with gateway gate
            pcfg.fetchIntervalNs      = 5'000'000LL;
            pcfg.isrCheckIntervalNs   = 10'000'000LL;
            pcfg.maxFetchBytes        = 1 << 20;

            // Determine the transport: real Aeron transport if available,
            // otherwise a per-replica InMemoryTransport (no-op: no peers wired).
            PartitionTransport* transport = replTransport_.get();
            if (!transport) {
                // Allocate a no-op InMemoryTransport owned by the replica registry.
                // InMemoryTransport with no registered leader/follower drops all frames.
                nullTransports_.push_back(std::make_unique<InMemoryTransport>());
                transport = nullTransports_.back().get();
            }

            auto replica = std::make_unique<PartitionReplica>(
                pcfg,
                Role::Follower,
                /*epoch=*/0,
                allNodeIds,
                allNodeIds,
                logPtr,
                transport
            );

            if (replTransport_) {
                // DR-BUG-5: register with composite (topicId, partitionId) key so
                // replicas from different topics with the same partition index do not
                // collide in the transport routing table.
                replTransport_->registerReplica(topicId, replica->partitionId(), replica.get());
            }

            replicaRegistry_->add(topicId, part, std::move(replica));
        }
    }

    if (replTransport_) {
        spdlog::info("BrokerNode[{}]: registered {} replicas with AeronPartitionTransport",
                     config_.nodeId, replicaRegistry_->replicaCount());
    }

    // -----------------------------------------------------------------------
    // Build replBridge_ — ReplicationMetadataBridge.
    // Always built (even without transport) so role transitions happen on
    // the duty cycle; this is a no-op on single-node (only 1 peer = self).
    // -----------------------------------------------------------------------
    replBridge_ = std::make_unique<ReplicationMetadataBridge>(
        selfId, allNodeIds, *replicaRegistry_);

    spdlog::info("BrokerNode[{}]: replication built ({} replicas, transport={})",
                 config_.nodeId,
                 replicaRegistry_->replicaCount(),
                 replTransport_ ? "aeron:udp" : "none");
}

// ---------------------------------------------------------------------------
// BrokerNode::buildGateway — create the ClientGateway and NodeRuntimeAgent.
//
// Called AFTER buildConsensusObjects() (so agent_ / nodeAgent_ are valid)
// and AFTER the aeron_ client is connected (so addSubscription succeeds).
//
// The leaderView lambda is safe: it reads this->isLeader() which is an atomic
// observer — safe to call from the gateway's duty-cycle thread (same thread
// as the AgentRunner).
//
// leaderView ruling (BR-7a / Milestone A):
//   isLeader = node.isLeader()
//   leaderClientEndpoint = isLeader ? config_.clientEndpoint.channel : ""
//   Effect: followers degrade to accept locally (empty endpoint path in BR-5).
//   TODO(Milestone B): per-peer client endpoints for exact REDIRECT.
// ---------------------------------------------------------------------------
void BrokerNode::buildGateway() {
    // Capture config_ by value (immutable after construction).
    const std::string clientChannel  = config_.clientEndpoint.channel;
    const int32_t     clientStreamId = config_.clientEndpoint.streamId;
    const int32_t     nodeId         = config_.nodeId;

    // leaderView: read atomic isLeader() + leaderClientEndpoint() (GB-4).
    //
    // Milestone-B ruling: supply per-peer client endpoints for exact REDIRECT.
    //   - leader → {isLeader=true, leaderClientEndpoint=""}  (no self-redirect)
    //   - follower + known leader endpoint → {isLeader=false, leaderClientEndpoint=<peer channel>}
    //   - follower + unknown endpoint → {isLeader=false, leaderClientEndpoint=""}
    //     (gateway degrades to local-accept for Connect; data ops return NotLeader)
    // Both isLeader() and leaderClientEndpoint() are observer-thread-safe (atomic reads).
    auto leaderViewFn = [this]() -> rune::gateway::LeaderView {
        const bool leader = this->isLeader();
        return rune::gateway::LeaderView{
            /*isLeader=*/leader,
            /*leaderClientEndpoint=*/leader ? std::string{} : this->leaderClientEndpoint()
        };
    };

    // Build the gateway (subscribes on clientChannel).
    try {
        gateway_ = std::make_unique<rune::gateway::ClientGateway>(
            aeron_,
            clientChannel,
            clientStreamId,
            nodeId,
            rune::gateway::ClientGateway::kDefaultSessionTimeoutNs,
            std::move(leaderViewFn)
        );
        // Wire in the TopicRegistry (GB-1). Non-owning pointer; registry outlives
        // the gateway (both destroyed in stop() with registry after gateway).
        gateway_->setTopicRegistry(topicRegistry_.get());

        // Wire in the ReplicaRegistry (GR-4). Non-owning pointer; replicaRegistry_
        // outlives the gateway (gateway_ is destroyed before replicaRegistry_ in stop()).
        // When set, handleProduce() will call onLeaderLogAppended() after each
        // append so the leader replica's tracked LEO/HW is reconciled and followers
        // can fetch the new bytes (Mode A seam — no double-append).
        gateway_->setReplicaRegistry(replicaRegistry_.get());

        // M1: Apply the cluster-majority minIsr gate for acks=All produces.
        // gatewayMinIsr is computed from the peer count (or RUNE_MIN_ISR env override)
        // during parseNodeConfig; single-node → 1 (leader-only ISR is sufficient).
        gateway_->setMinIsr(config_.gatewayMinIsr);
        spdlog::info("BrokerNode[{}]: gateway minIsr set to {} (cluster size={})",
                     nodeId, config_.gatewayMinIsr, config_.peers.size());
    } catch (const std::exception& ex) {
        spdlog::warn("BrokerNode[{}]: ClientGateway construction failed: {}; proceeding without gateway",
                     nodeId, ex.what());
        gateway_.reset();
    }

    // -----------------------------------------------------------------------
    // CG-6 wiring: GroupCoordinationService → ClientGateway.
    //
    // Build a durable __consumer_offsets Log + single-partition PartitionReplica
    // (InMemoryTransport — role transitions handled by the bridge like other
    // replicas; no Aeron transport needed for the offset log in CG-6).
    // Then create ReplicatedOffsetStore + GroupCoordinationService and wire
    // both directions: gateway → service (setGroupService) and
    // service → gateway (setGateway).
    //
    // This must happen AFTER gateway_ is set (we wire into it), and the
    // GroupCoordinationService is passed to NodeRuntimeAgent so doWork
    // drives the group coordinator duty cycle on the single-writer thread.
    // -----------------------------------------------------------------------
    if (gateway_) {
        try {
            const fs::path offsetDir = fs::path{config_.dataDir} / "__consumer_offsets";
            fs::create_directories(offsetDir);

            // One partition Log for all committed offsets (compact key/value encoding).
            offsetLog_ = std::make_unique<rune::storage::Log>(offsetDir.string());
            offsetLog_->recover();

            // Wrap in a PartitionReplica for the __consumer_offsets topic.
            // topicId=-1 is the sentinel for the offset log (not a user topic).
            // When replTransport_ is available (multi-node cluster), use it so
            // committed offsets are replicated to all peers and survive leader
            // failover.  On single-node (no transport), fall back to InMemoryTransport.
            // The replica is added to replicaRegistry_ so the metadata bridge drives
            // its role transitions (Follower → Leader on election, Leader → Follower
            // on dethronement) exactly like user-topic replicas.
            // Role starts Follower / epoch 0; the bridge corrects on first doWork.
            const NodeId selfId = static_cast<NodeId>(config_.nodeId);

            // Rebuild allNodeIds (same as in buildReplication).
            std::vector<NodeId> allNodeIds;
            allNodeIds.reserve(config_.peers.size());
            for (const auto& peer : config_.peers) {
                allNodeIds.push_back(static_cast<NodeId>(peer.id));
            }

            rune::replication::PartitionConfig ocfg;
            ocfg.topicId              = -1;   // sentinel: not a user topic
            ocfg.partitionId          = 0;
            ocfg.selfId               = selfId;
            // minIsr=1: offset commits are leader-local (no ISR majority wait).
            // The replica still replicates to followers via Aeron so that a new
            // leader can rebuild committed offsets after failover.  Leader-local
            // (minIsr=1) avoids ISR-establishment delays on the new leader before
            // the first offset commit succeeds.
            ocfg.minIsr               = 1;
            ocfg.replicaLagMaxMs      = config_.replicaLagMaxMs;
            ocfg.replicaLagMaxOffset  = 10'000;
            ocfg.fetchIntervalNs      = 5'000'000LL;
            ocfg.isrCheckIntervalNs   = 10'000'000LL;
            ocfg.maxFetchBytes        = 1 << 20;
            // CP-1: __consumer_offsets is a compacted topic (keep latest per key).
            ocfg.cleanupPolicy        = rune::replication::CleanupPolicy::Compact;

            // Transport: real Aeron transport if available, else no-op InMemoryTransport.
            rune::replication::PartitionTransport* offsetTransport = replTransport_.get();
            if (!offsetTransport) {
                nullTransports_.push_back(std::make_unique<rune::replication::InMemoryTransport>());
                offsetTransport = nullTransports_.back().get();
            }

            offsetReplica_ = std::make_unique<rune::replication::PartitionReplica>(
                ocfg,
                rune::replication::Role::Follower, // bridge promotes to Leader on first doWork
                /*epoch=*/0,
                allNodeIds,
                allNodeIds,
                offsetLog_.get(),
                offsetTransport);

            // Register with the Aeron transport (topicId=-1, partitionId=0) so
            // FetchRequest / FetchResponse frames for the offset log are routed here.
            if (replTransport_) {
                replTransport_->registerReplica(-1, 0, offsetReplica_.get());
            }

            // Add to replicaRegistry_ so the metadata bridge drives role transitions
            // and NodeRuntimeAgent::doWork calls replica->doWork() each cycle.
            if (replicaRegistry_) {
                replicaRegistry_->add(-1, 0, std::move(offsetReplica_));
                // After move, recover the raw pointer for offsetStore_ construction.
                offsetReplica_.reset(); // unique_ptr now owned by replicaRegistry_
            }
            // Re-acquire raw pointer from registry for offsetStore_ construction.
            rune::replication::PartitionReplica* offsetReplicaRaw =
                replicaRegistry_ ? replicaRegistry_->lookup(-1, 0) : nullptr;

            // ReplicatedOffsetStore: commits append to offsetReplicaRaw + in-memory map.
            // offsetReplicaRaw is now owned by replicaRegistry_; offsetReplica_ is null.
            offsetStore_ = std::make_unique<rune::coordination::ReplicatedOffsetStore>(
                offsetReplicaRaw);
            // Replay any previously committed offsets from the local Log.
            // On a fresh node this is a no-op; on restart it restores durable state.
            // On failover, GroupCoordinationService::doWork() will rebuild on
            // becoming leader so the in-memory index reflects replicated commits.
            offsetStore_->rebuildFromLog();

            // GroupCoordinationService: handles JoinGroup / SyncGroup / Heartbeat /
            // LeaveGroup / OffsetCommit / OffsetFetch for all consumer groups.
            groupCoordService_ = std::make_unique<GroupCoordinationService>(
                offsetStore_.get(),
                topicRegistry_.get(),
                [this]() -> bool { return this->isLeader(); },
                [this]() -> NodeId { return static_cast<NodeId>(this->leaderId()); });

            // Wire both directions.
            gateway_->setGroupService(groupCoordService_.get());
            groupCoordService_->setGateway(gateway_.get());

            spdlog::info("BrokerNode[{}]: GroupCoordinationService wired (offsets at {})",
                         nodeId, offsetDir.string());
        } catch (const std::exception& ex) {
            spdlog::error("BrokerNode[{}]: GroupCoordinationService setup failed: {}; "
                          "group consumers will not work", nodeId, ex.what());
            groupCoordService_.reset();
            offsetStore_.reset();
            // offsetReplica_ may have been moved into replicaRegistry_ already;
            // reset() is safe either way (no-op if already null).
            offsetReplica_.reset();
            offsetLog_.reset();
        }
    }

    // -----------------------------------------------------------------------
    // TX-3: Build TransactionCoordinationService + replicated txn state store.
    // Mirrors the GroupCoordinationService wiring above (CG-5/CG-6).
    //
    // __transaction_state: topicId=-2, partitionId=0 (sentinels, not user topics).
    // minIsr=2 (majority): txn state transitions are majority-durable (unlike
    // offsets which use minIsr=1 leader-local for lower latency).
    // -----------------------------------------------------------------------
    if (gateway_) {
        try {
            const fs::path txnDir = fs::path{config_.dataDir} / "__transaction_state";
            fs::create_directories(txnDir);

            txnLog_ = std::make_unique<rune::storage::Log>(txnDir.string());
            txnLog_->recover();

            const NodeId selfId = static_cast<NodeId>(config_.nodeId);

            std::vector<NodeId> allNodeIds;
            allNodeIds.reserve(config_.peers.size());
            for (const auto& peer : config_.peers) {
                allNodeIds.push_back(static_cast<NodeId>(peer.id));
            }

            rune::replication::PartitionConfig tcfg;
            tcfg.topicId              = -2;  // sentinel: __transaction_state
            tcfg.partitionId          = 0;
            tcfg.selfId               = selfId;
            tcfg.minIsr               = std::min(2, static_cast<int>(allNodeIds.size()));
            tcfg.replicaLagMaxMs      = config_.replicaLagMaxMs;
            tcfg.replicaLagMaxOffset  = 10'000;
            tcfg.fetchIntervalNs      = 5'000'000LL;
            tcfg.isrCheckIntervalNs   = 10'000'000LL;
            tcfg.maxFetchBytes        = 1 << 20;
            // CP-1: __transaction_state is a compacted topic (keep latest per txnId).
            tcfg.cleanupPolicy        = rune::replication::CleanupPolicy::Compact;

            rune::replication::PartitionTransport* txnTransport = replTransport_.get();
            if (!txnTransport) {
                nullTransports_.push_back(
                    std::make_unique<rune::replication::InMemoryTransport>());
                txnTransport = nullTransports_.back().get();
            }

            txnReplica_ = std::make_unique<rune::replication::PartitionReplica>(
                tcfg,
                rune::replication::Role::Follower,
                /*epoch=*/0,
                allNodeIds,
                allNodeIds,
                txnLog_.get(),
                txnTransport);

            if (replTransport_) {
                replTransport_->registerReplica(-2, 0, txnReplica_.get());
            }

            if (replicaRegistry_) {
                replicaRegistry_->add(-2, 0, std::move(txnReplica_));
                txnReplica_.reset();
            }

            rune::replication::PartitionReplica* txnReplicaRaw =
                replicaRegistry_ ? replicaRegistry_->lookup(-2, 0) : nullptr;
            if (!txnReplicaRaw && txnReplica_) {
                txnReplicaRaw = txnReplica_.get(); // single-node path (no registry)
            }

            txnState_ = std::make_unique<rune::transaction::ReplicatedTxnStateStore>(
                txnReplicaRaw);
            txnState_->rebuildFromLog();

            txnCoordService_ = std::make_unique<TransactionCoordinationService>(
                txnState_.get(),
                topicRegistry_.get(),
                [this]() -> bool { return this->isLeader(); },
                [this]() -> NodeId { return static_cast<NodeId>(this->leaderId()); });

            txnCoordService_->setGateway(gateway_.get());

            gateway_->setTxnService(txnCoordService_.get());

            spdlog::info("BrokerNode[{}]: TransactionCoordinationService wired (txn state at {})",
                         nodeId, txnDir.string());
        } catch (const std::exception& ex) {
            spdlog::error("BrokerNode[{}]: TransactionCoordinationService setup failed: {}; "
                          "transactions will not work", nodeId, ex.what());
            txnCoordService_.reset();
            txnState_.reset();
            txnReplica_.reset();
            txnLog_.reset();
        }
    }

    // Build the composite runtime agent:
    //   single-node: wraps agent_    (ConsensusAgent)
    //   multi-node:  wraps nodeAgent_ (NodeConsensusAgent)
    rune::consensus::Agent& innerAgent =
        nodeAgent_ ? static_cast<rune::consensus::Agent&>(*nodeAgent_)
                   : static_cast<rune::consensus::Agent&>(*agent_);

    runtimeAgent_ = std::make_unique<NodeRuntimeAgent>(
        innerAgent,
        election_.get(),          // GR-2: election for leaderMemberId/leadershipTermId reads
        replBridge_.get(),        // GR-2: metadata bridge (may be null if no replicas)
        replTransport_.get(),     // GR-3: replication transport (may be null)
        replicaRegistry_.get(),   // GR-3: replica registry (may be null if no topics)
        gateway_.get(),
        groupCoordService_.get(), // CG-6: group coordinator (null-safe if setup failed)
        txnCoordService_.get());  // TX-3: txn coordinator (null-safe if setup failed)

    // AgentRunner drives the NodeRuntimeAgent (consensus + gateway on ONE thread).
    agentRunner_ = std::make_unique<rune::consensus::AgentRunner>(
        *runtimeAgent_, idleStrategy_, clock_);
}

// ---------------------------------------------------------------------------
// BrokerNode::awaitLeader
// ---------------------------------------------------------------------------
bool BrokerNode::awaitLeader(std::chrono::milliseconds timeout) {
    using clock = std::chrono::steady_clock;
    const auto deadline = clock::now() + timeout;

    // For single-node: wait for isLeader().
    // For multi-node: wait for leaderId() >= 0 (any leader, leader or follower).
    auto hasLeader = [this]() -> bool {
        if (nodeAgent_) {
            return nodeAgent_->leaderId() >= 0;
        }
        return isLeader();
    };

    while (clock::now() < deadline) {
        if (hasLeader()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    return hasLeader(); // one final check at deadline
}

// ---------------------------------------------------------------------------
// BrokerNode::isLeader
// ---------------------------------------------------------------------------
bool BrokerNode::isLeader() const noexcept {
    if (!election_) return false;

    // For the multi-node path, rely SOLELY on the atomic observedLeaderId_ mirror
    // written by NodeConsensusAgent::updateObservedLeader() after each duty cycle.
    // This avoids reading TWO independent atomics (stateCode_ + observedLeaderId_)
    // which have a TOCTOU race: the duty-cycle thread could transition the state
    // between the two reads, causing isLeader() to return false for a node that IS
    // the leader but is momentarily in a pass-through state (LeaderReplay, LeaderInit)
    // not covered by the stateCode_ check — which produced the observed ~1/14 flake.
    if (nodeAgent_) {
        return nodeAgent_->leaderId() == static_cast<NodeId>(config_.nodeId);
    }

    // Single-node (no nodeAgent_): fall back to stateCode_.
    // All leader states including pass-through ones: LeaderLogReplication, LeaderReplay,
    // LeaderInit, LeaderReady, Closed.  Missing LeaderReplay/LeaderInit here was safe for
    // single-node (their pass-through is near-instantaneous and the caller is the same
    // thread that drives doWork), but we include them for correctness completeness.
    // Use the atomic stateCode() — release-written by the agent thread on every
    // state transition — instead of the non-atomic state() accessor.
    const int32_t c = election_->stateCode();
    return c == static_cast<int32_t>(ElectionState::LeaderLogReplication) ||
           c == static_cast<int32_t>(ElectionState::LeaderReplay)         ||
           c == static_cast<int32_t>(ElectionState::LeaderInit)           ||
           c == static_cast<int32_t>(ElectionState::LeaderReady)          ||
           c == static_cast<int32_t>(ElectionState::Closed);
}

// ---------------------------------------------------------------------------
// BrokerNode::leaderId
// ---------------------------------------------------------------------------
NodeId BrokerNode::leaderId() const noexcept {
    // Multi-node path: read the atomic mirror updated by NodeConsensusAgent.
    if (nodeAgent_) {
        return nodeAgent_->leaderId();
    }
    // Single-node (BR-2) path: derive from atomic stateCode.
    if (isLeader()) return static_cast<NodeId>(config_.nodeId);
    return -1; // kNullValue — no leader known yet
}

// ---------------------------------------------------------------------------
// BrokerNode::leadershipTermId
// ---------------------------------------------------------------------------
Term BrokerNode::leadershipTermId() const noexcept {
    if (!election_) return 0;
    // election_->leadershipTermId() is written on the duty-cycle thread; reading
    // it from an observer thread (main's polling loop) is a benign data race on
    // a plain Term (int64_t) — same pattern used by leaderId() observers.  We
    // accept the rare torn-read risk here because this value is used only for
    // logging and the oracle filter, never for consensus decisions.
    return election_->leadershipTermId();
}

// ---------------------------------------------------------------------------
// BrokerNode::publishRole — update the Aeron node-role + leadership-term
// counters (RoleCounter.h) with this node's current view.
//
// Machine-readable leadership signal: the dr-e2e harness reads these counters
// out of the CnC file via rune_role instead of scraping "is LEADER" log lines
// (which are log-level dependent and fragile).  Mirrors Aeron Cluster, which
// publishes node role + leadership term as counters.
//
// Resolves the pending addCounter registrations lazily: the embedded driver
// acks counter allocation asynchronously, so findCounter() may return nullptr
// for the first few calls — we simply retry on the next poll tick.  Once
// resolved, the Counter is cached and set() is a single relaxed store.
//
// Observer-thread-safe: reads the same atomic isLeader()/leaderId()/term
// accessors the leadership log uses.  Called from main's 100 ms poll loop.
// ---------------------------------------------------------------------------
void BrokerNode::publishRole() noexcept {
    if (!aeron_) return;

    // Resolve pending counter registrations once the driver has acked them.
    if (!roleCounter_ && roleCounterRegId_ >= 0) {
        try { roleCounter_ = aeron_->findCounter(roleCounterRegId_); }
        catch (...) { roleCounter_.reset(); }  // driver error — retry next tick
    }
    if (!termCounter_ && termCounterRegId_ >= 0) {
        try { termCounter_ = aeron_->findCounter(termCounterRegId_); }
        catch (...) { termCounter_.reset(); }
    }

    // Determine this node's current role.  We expose Leader vs Follower vs None;
    // Candidate is not separately observable via the BrokerNode accessors, so a
    // node that observes no leader yet is reported as None (pre-election gap).
    const bool leader = isLeader();
    const NodeId lid  = leaderId();
    const RoleCode role = leader ? RoleCode::Leader
                        : (lid >= 0 ? RoleCode::Follower : RoleCode::None);

    if (roleCounter_) {
        roleCounter_->set(static_cast<std::int64_t>(role));
    }
    if (termCounter_) {
        // Publish the term this node observes (its own leadership term id).
        termCounter_->set(static_cast<std::int64_t>(leadershipTermId()));
    }
}

// ---------------------------------------------------------------------------
// BrokerNode::leaderClientEndpoint (GB-4)
// ---------------------------------------------------------------------------
std::string BrokerNode::leaderClientEndpoint() const noexcept {
    // If this node is the leader, no redirect to self.
    if (isLeader()) return {};

    const NodeId lid = leaderId();
    if (lid < 0) return {};  // leader unknown — no endpoint

    // Find the leader peer in the immutable config and return its clientChannel.
    // config_.peers is sorted by id, so linear scan is bounded and fast.
    for (const auto& peer : config_.peers) {
        if (static_cast<NodeId>(peer.id) == lid) {
            return peer.clientChannel();  // empty if clientPort == 0
        }
    }
    return {};  // leader id not found in peer list (should not happen)
}

// ---------------------------------------------------------------------------
// BrokerNode::stop
// ---------------------------------------------------------------------------
void BrokerNode::stop() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true,
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return; // idempotent
    }

    // 1. Stop the agent runner (signal + join the duty-cycle thread first).
    if (agentRunner_) {
        agentRunner_->stopAndJoin();
    }

    // 2. Release the runtime agent and gateway FIRST — in this order:
    //      a. runtimeAgent_ (holds refs to nodeAgent_/agent_ + gateway_ + replication)
    //      b. gateway_ (owns the client ingress Subscription + pending egress
    //                   ExclusivePublications — must deregister before aeron_.reset())
    //      c. replication objects (replBridge_ → replicaRegistry_ → replTransport_)
    //         replTransport_ owns Aeron Subscription + ExclusivePublications
    //    BR-3b teardown ordering: AgentRunner stop → release gateway/nodeAgent/ingress/sink
    //    → close aeron → stop driver.
    runtimeAgent_.reset();    // composite agent (holds refs to inner agent + gateway + replication)
    // TX-3: destroy txn coordination service before gateway (holds gateway* + txnState*).
    txnCoordService_.reset(); // TransactionCoordinationService (no Aeron resources, holds gateway*)
    // CG-6: destroy group coordination service before gateway (it holds a gateway* ref).
    groupCoordService_.reset(); // GroupCoordinationService (no Aeron resources, holds gateway* + offsets*)
    gateway_.reset();         // ClientGateway: deregisters Subscription + ExclusivePubs
    // TX-3: destroy txn state store before replicaRegistry_ (holds raw ptr to txn replica).
    txnState_.reset();        // ReplicatedTxnStateStore (holds raw ptr to txn replica in registry)
    txnReplica_.reset();      // no-op: ownership transferred to replicaRegistry_ in buildGateway
    txnLog_.reset();          // Log for __transaction_state (file flush on dtor)
    // CG-6: destroy offset store before replicaRegistry_ (offsetStore_ holds raw ptr
    //       to the offsetReplica which replicaRegistry_ owns after CG-6 refactor).
    offsetStore_.reset();     // ReplicatedOffsetStore (holds raw ptr to offset replica in registry)
    offsetReplica_.reset();   // no-op: ownership transferred to replicaRegistry_ in buildGateway
    offsetLog_.reset();       // Log for __consumer_offsets (file flush on dtor)
    replBridge_.reset();      // ReplicationMetadataBridge (no Aeron resources)
    replicaRegistry_.reset(); // PartitionReplicas + offsetReplica (destroyed before transport)
    replTransport_.reset();   // AeronPartitionTransport: deregisters replication Subscription + ExclusivePubs
    nullTransports_.clear();  // Per-replica no-op transports (no Aeron resources)
    topicRegistry_.reset();   // TopicRegistry: Logs are files (flushed/synced by Log dtor)

    // 3. Release consensus objects that own Aeron publications / subscription.
    //    Must happen BEFORE closing the Aeron client and media driver so the
    //    client shuts down cleanly (publications/subscriptions deregistered while
    //    the conductor is still running).
    nodeAgent_.reset();      // composite agent (holds refs to aeronIngress_ + agent_)
    udpSink_.reset();        // ExclusivePublications (deregisters from conductor)
    aeronIngress_.reset();   // Subscription (deregisters from conductor)
    agent_.reset();          // ConsensusAgent (refs election_ + ingressRing_)
    election_.reset();
    publisher_.reset();
    ingressRing_.reset();
    nodeStateFile_.reset();
    termLog_.reset();

    // 3b. Release the Aeron node-role/term counters BEFORE closing the client.
    //     ~Counter deregisters from the conductor, so it must run while the
    //     conductor thread is still alive — otherwise the Counter dtor locks a
    //     destroyed conductor mutex (recursive_mutex lock failed) at process
    //     teardown.  Reset these here (not in ~BrokerNode) to fix that ordering.
    roleCounter_.reset();
    termCounter_.reset();

    // 4. Close the Aeron client (joins the C++ conductor thread).
    //    After aeron_.reset() all "remove publication / subscription / close client"
    //    commands have been written into the CnC ring by the conductor's onClose().
    if (aeron_) {
        aeron_.reset();
    }

    // 5. Stop the embedded media driver.
    //    stopEmbeddedDriver() first joins the driver thread (so we are the ONLY
    //    thread touching the driver afterwards), then drains any final CnC commands
    //    that the conductor posted during step 3 before calling aeron_driver_close().
    stopEmbeddedDriver();

    spdlog::info("BrokerNode: node {} stopped", config_.nodeId);
}

// ---------------------------------------------------------------------------
// BrokerNode::startEmbeddedDriver
// ---------------------------------------------------------------------------
void BrokerNode::startEmbeddedDriver() {
    if (aeron_driver_context_init(&driverCtx_) < 0) {
        throw std::runtime_error(
            std::string("BrokerNode: aeron_driver_context_init failed: ") +
            aeron_errmsg());
    }

    // Use a node-specific aeron dir to avoid collisions in tests.
    const std::string aeronDir =
        (fs::path{config_.dataDir} / "aeron").string();
    aeron_driver_context_set_dir(driverCtx_, aeronDir.c_str());

    // SHARED threading: one background thread for sender+receiver+conductor.
    aeron_driver_context_set_threading_mode(driverCtx_,
                                            AERON_THREADING_MODE_SHARED);
    // Clean up on start/shutdown so tests don't leave stale shm files.
    aeron_driver_context_set_dir_delete_on_start(driverCtx_, true);
    aeron_driver_context_set_dir_delete_on_shutdown(driverCtx_, true);
    // Lightweight idle in the driver thread.
    aeron_driver_context_set_shared_idle_strategy(driverCtx_, "sleep-ns");
    // Small term buffers — plenty for a 1-node broker.
    aeron_driver_context_set_term_buffer_sparse_file(driverCtx_, true);
    aeron_driver_context_set_term_buffer_length(driverCtx_, 64 * 1024);

    if (aeron_driver_init(&driver_, driverCtx_) < 0) {
        aeron_driver_context_close(driverCtx_);
        driverCtx_ = nullptr;
        throw std::runtime_error(
            std::string("BrokerNode: aeron_driver_init failed: ") +
            aeron_errmsg());
    }

    // manual_main_loop=true: we drive it from our own thread.
    if (aeron_driver_start(driver_, /*manual_main_loop=*/true) < 0) {
        aeron_driver_close(driver_);
        aeron_driver_context_close(driverCtx_);
        driver_    = nullptr;
        driverCtx_ = nullptr;
        throw std::runtime_error(
            std::string("BrokerNode: aeron_driver_start failed: ") +
            aeron_errmsg());
    }

    driverRunning_.store(true, std::memory_order_release);
    driverThread_ = std::thread([this]() {
        while (driverRunning_.load(std::memory_order_acquire)) {
            aeron_driver_main_idle_strategy(
                driver_,
                aeron_driver_main_do_work(driver_));
        }
    });

    // -----------------------------------------------------------------------
    // Driver-ready bounded poll: pump aeron_driver_main_do_work() until the
    // conductor has written its CnC file and is accepting commands, then
    // wait for Aeron::connect() to succeed.  This replaces the fixed 50 ms
    // sleep and is bounded so it never spins indefinitely.
    //
    // The Aeron C API does not expose a public "conductor ready" predicate, so
    // we probe by attempting to connect the Aeron client.  connect() returns
    // nullptr only when the CnC file is not yet present / valid; once it
    // returns non-null the conductor is ready.
    //
    // We warm-pump the driver loop from THIS thread in 1 ms slices (up to
    // kDriverReadyMaxMs) so the conductor's init work is done before the
    // Aeron client constructor blocks waiting for the CnC file.
    // -----------------------------------------------------------------------
    static constexpr int kDriverReadyMaxMs    = 200; // absolute upper bound
    static constexpr int kDriverReadySliceUs  = 500; // poll granularity (0.5 ms)

    const std::string aeronDirStr =
        (fs::path{config_.dataDir} / "aeron").string();

    const auto readyDeadline =
        std::chrono::steady_clock::now() +
        std::chrono::milliseconds{kDriverReadyMaxMs};

    // The CnC file is created by the driver during init. Poll until it appears.
    while (std::chrono::steady_clock::now() < readyDeadline) {
        // Pump the driver loop from this thread.
        aeron_driver_main_do_work(driver_);

        // Check if the CnC file has appeared.
        const std::string cncPath = aeronDirStr + "/cnc.dat";
        if (fs::exists(cncPath)) {
            // Give the conductor a couple more pumps to finish writing the file.
            aeron_driver_main_do_work(driver_);
            aeron_driver_main_do_work(driver_);
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds{kDriverReadySliceUs});
    }

    spdlog::info("BrokerNode: embedded Aeron driver started (dir={})",
                 aeron_driver_context_get_dir(driverCtx_));
}

// ---------------------------------------------------------------------------
// BrokerNode::stopEmbeddedDriver
// ---------------------------------------------------------------------------
void BrokerNode::stopEmbeddedDriver() noexcept {
    // Step A: Signal the driver loop thread to stop and join it.
    //
    // After stop() has called aeron_.reset() (which joins the C++ conductor
    // thread), the conductor's onClose() has queued "remove publication /
    // subscription / close client" commands into the driver's CnC ring.
    // The driver thread may or may not have processed these commands before we
    // signal it to stop — that is fine: we will drain them below in step B
    // AFTER the thread exits, from the calling thread only, so there is no
    // concurrent access to aeron_driver_main_do_work.
    if (driverRunning_.exchange(false, std::memory_order_acq_rel)) {
        if (driverThread_.joinable()) {
            driverThread_.join(); // driver thread has fully exited here
        }
    }

    // Step B: Drain any CnC commands the conductor posted that the driver
    // thread did not yet process.  At this point driverThread_ is joined so
    // we are the ONLY thread calling aeron_driver_main_do_work — no data race.
    //
    // This prevents aeron_driver_conductor_on_close() (inside aeron_driver_close)
    // from encountering live publication/subscription entries that the C++ client
    // objects already freed on their side, which would cause a use-after-free /
    // SIGABRT visible as a non-zero process exit AFTER gtest prints "[ PASSED ]".
    if (driver_) {
        static constexpr int kDrainMaxMs   = 50;  // hard cap (generous upper bound)
        static constexpr int kDrainSliceUs = 200; // inter-pump sleep when busy
        const auto drainDeadline =
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds{kDrainMaxMs};
        while (std::chrono::steady_clock::now() < drainDeadline) {
            const int work = aeron_driver_main_do_work(driver_);
            if (work == 0) {
                // One extra pump to let the conductor flush any final responses.
                aeron_driver_main_do_work(driver_);
                break;
            }
            std::this_thread::sleep_for(std::chrono::microseconds{kDrainSliceUs});
        }
    }

    // Step C: Close the driver (frees conductor/sender/receiver state).
    // aeron_driver_conductor_on_close now finds empty publication/subscription
    // tables (drained in step B) so no use-after-free occurs.
    // If it still returns -1 for any benign reason (e.g. the dir-delete path
    // fails because TempDir already removed it), log and absorb — do NOT
    // propagate the error to the process exit code.
    if (driver_) {
        if (aeron_driver_close(driver_) < 0) {
            spdlog::warn("BrokerNode: aeron_driver_close returned error (node {}): {}",
                         config_.nodeId, aeron_errmsg());
        }
        driver_ = nullptr;
    }

    // Step D: Close the driver context (unmaps CnC, optionally deletes aeron dir).
    // Same benign-error-absorb policy as step C.
    if (driverCtx_) {
        if (aeron_driver_context_close(driverCtx_) < 0) {
            spdlog::warn("BrokerNode: aeron_driver_context_close returned error (node {}): {}",
                         config_.nodeId, aeron_errmsg());
        }
        driverCtx_ = nullptr;
    }
}

} // namespace rune::node
