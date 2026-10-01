#include <rune/node/BrokerNode.h>
#include <rune/replication/ReplicationFrameLimits.h>
#include <rune/node/RoleCounter.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// CG-6 wiring: storage + replication headers for __consumer_offsets.
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/storage/InMemoryLog.h>
// TX-3 wiring: transaction coordination service + replicated state store.
#include <rune/node/TransactionCoordinationService.h>
#include <rune/transaction/ReplicatedTxnStateStore.h>
// MD1 Task 4: MetadataService for __cluster_metadata.
#include <rune/node/MetadataService.h>
// Task 6: AdminWorker + SchemaRegistry + __topic_schemas persistence.
#include <rune/node/AdminWorker.h>
#include <rune/node/SchemaRegistry.h>
// Task 6: RecordBatch reader for rebuildFromLog (same as TxnStateStore).
#include <rune/protocol/RecordBatch.h>
// Task 8a: protocol response frames for DDL egress (respondDefine/respondGet).
#include <rune/protocol/client/SchemaMessages.h>
#include <cstdlib>
#include <cstring>

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

    // Spec §3.3: shard-mode misconfiguration is a HARD startup refusal, before any
    // side effect (no dataDir, no driver, no ports).
    if (auto err = config_.validateShardConfig(); err.has_value()) {
        throw std::runtime_error("BrokerNode[" + std::to_string(config_.nodeId) +
                                 "]: refusing to start — " + *err);
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
            config_.effectiveAeronDir();
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

    // 7.0 fsyncgate: a log whose recovery failed (index msync / dir fsync — the
    //     log is marked storage-failed) must never serve.  Refuse to start —
    //     critical log with the path, the halt path (exit 74), nothing served.
    refuseStartOnFailedRecovery_();

    // 7a. Self-contained worker shards (spec §3.1; Task 9 fix round 1 I2): the
    //     per-worker invoker drivers + clients are created only NOW — after every
    //     long phase (consensus build, offset/txn/metadata rebuilds, admin build)
    //     — then their planes/ingress/egress are resolved (pumped by each shard's
    //     own AeronPump on THIS thread) and the shard runners start right after.
    //     Nothing ever leaves a shard driver/client unpumped for a long phase
    //     (a gap > the client liveness timeout kills the client).  Each ShardAgent
    //     takes pump ownership on its worker thread's first pass.
    if (config_.shardMode()) {
        buildShardRuntimes_();
        attachShardRuntimes_();
    }

    // P2 (Task 10 round 4): switch the pool to ring-delivered ownership BEFORE
    //    the control thread exists — runnersStarted_ is control-thread state and
    //    is never written while the control thread runs.  SetOwnership ops the
    //    control thread enqueues before a worker runner starts are applied, in
    //    FIFO order, on that worker's first pass.
    if (workerPool_) workerPool_->armRingOwnership();

    // 7. Start the agent runner thread (drives NodeRuntimeAgent, which drives
    //    consensus + gateway on the single-writer thread).
    agentRunner_->start();

    // 7b. E2 inc2b: start per-worker AgentRunner threads.
    //     Must start AFTER buildGateway() (owned sets published in buildWorkerPool
    //     which is called from buildGateway) and AFTER agentRunner_->start()
    //     (control thread must be live before workers start draining ingress rings).
    //     Workers are independent of the control agent but their ingress rings are
    //     fed by the control thread, so the ordering: control first, workers after.
    if (workerPool_) {
        if (config_.shardMode()) {
            // Busy-spin ONLY when pinned to a dedicated core (principle §1a.3);
            // unpinned shard workers use the backoff idle.
            workerPool_->startShardRunners(shardAgents_, clock_,
                                           /*busySpin=*/config_.workerCoreBase >= 0,
                                           config_.idleMaxParkUs);
        } else {
            workerPool_->startRunners(config_.workerIdleStrategy, clock_, config_.idleMaxParkUs);
        }
        spdlog::info("BrokerNode[{}]: {} worker thread(s) started (shard={})",
                     config_.nodeId, workerPool_->workerCount(), config_.shardMode());
    }

    // 7c. Task 6: start the AdminWorker AgentRunner thread.
    //     Must start AFTER agentRunner_ (control thread drives replication for
    //     __topic_schemas log; admin thread calls appendDurable which uses the
    //     replica).  Separate thread — never blocks the gateway duty cycle.
    if (adminWorkerRunner_) {
        adminWorkerRunner_->start();
        spdlog::info("BrokerNode[{}]: AdminWorker thread started", config_.nodeId);
    }

    // 8. Start the LogCleanerAgent on its own thread (CP-6).
    //    Must start AFTER agentRunner_ so the duty cycle begins draining swaps
    //    before the cleaner can enqueue any (avoids a window where a swap is
    //    enqueued but never drained if agentRunner_ hasn't started yet).
    if (cleanerRunner_) {
        cleanerRunner_->start();
    }

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
    // E5 Task 5: per-worker replication — pre-flight validation.
    //
    // Hard-refuse to start with invalid config when perWorkerReplication=true:
    //   (a) Stream range collision: replBaseStreamId range must not overlap
    //       client/consensus/workerIngress/workerEgress ranges.
    //   (b) Asymmetric worker count: every peer that has advertised a worker count
    //       must match this node's count; partial-knowledge peers (wc==0) are
    //       tolerated (they will be re-checked on membership change).
    //
    // Neither validation requires aeron_ to be live; they gate the hard invariants
    // that cannot be safely recovered from at runtime (stream collision = silent
    // message loss; asymmetric W = split replication planes).
    // -----------------------------------------------------------------------
    if (config_.perWorkerReplication) {
        if (auto err = config_.validateStreamRanges(); err.has_value()) {
            throw std::runtime_error(
                "BrokerNode[" + std::to_string(config_.nodeId) +
                "]: per-worker replication stream range collision — " + *err);
        }

        // Build (nodeId, workerCount) peer list for symmetric-count check.
        std::vector<std::pair<int32_t, int32_t>> wPeers;
        wPeers.reserve(config_.peers.size());
        for (const auto& p : config_.peers) {
            if (p.id == config_.nodeId) continue;   // skip self
            if (p.workerCount != 0)                 // 0 = not yet advertised
                wPeers.push_back({p.id, p.workerCount});
        }
        auto wCheck = validateSymmetricWorkerCount(config_.workerCount, wPeers);
        if (!wCheck.ok) {
            throw std::runtime_error(
                "BrokerNode[" + std::to_string(config_.nodeId) +
                "]: per-worker replication refused — " + wCheck.error);
        }
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
                aeron_, ingressChannel, ingressStream, selfId,
                config_.replicationOutboundRingBytes);
            // F18: every replication publication uses the embedded driver's term
            // (no channel overrides term-length) — fixed before any worker runs.
            replTransport_->setPublicationMaxMessageBytes(
                rune::replication::aeronMaxMessageLength(config_.aeronTermBufferBytes));

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
    // E5 Task 5: per-worker replication transports.
    //
    // When perWorkerReplication is true AND aeron_ is live AND there are
    // replication peers, construct one WorkerReplicationTransport per worker.
    // Each transport W subscribes on stream (replBaseStreamId + W) and publishes
    // to each peer's replication channel on the same stream offset.
    //
    // Single-node / no-peers: skip — nothing to replicate, and the brief
    // explicitly says "do NOT build per-worker planes" when peers is empty
    // (WorkerReplicationTransport::allResolved() returns false on empty peers).
    //
    // Hard-refuse if any transport fails to resolve: a half-wired replication
    // plane silently drops messages for the partitions assigned to that worker.
    // -----------------------------------------------------------------------
    if (config_.shardMode()) {
        // Self-contained worker shards (spec §3.1/§3.3): one DEFERRED plane per
        // worker (replicas bind their concrete transport pointer now).  The planes
        // attach to their shard's own client in attachShardRuntimes_(), after the
        // long startup phases (fix round 1 I2).  Single node: no planes.
        bool hasPeers = false;
        for (const auto& peer : config_.peers) hasPeers |= (peer.id != config_.nodeId);
        if (hasPeers) {
            const int32_t wc = config_.workerCount >= 1 ? config_.workerCount : 1;
            perWorkerReplTransports_.reserve(static_cast<std::size_t>(wc));
            for (int32_t w = 0; w < wc; ++w)
                perWorkerReplTransports_.push_back(std::make_unique<WorkerReplicationTransport>(
                    w, config_.shardReplBindChannel(w), config_.replBaseStreamId + w,
                    WorkerReplicationTransport::DeferredAttach{}));
        }
    } else if (config_.perWorkerReplication && aeron_) {
        // Collect (peerId, peerChannel) pairs for all peers with a replication port.
        std::vector<std::pair<rune::replication::NodeId, std::string>> replPeers;
        replPeers.reserve(config_.peers.size());
        for (const auto& peer : config_.peers) {
            if (peer.id == config_.nodeId) continue;
            if (peer.replicationPort == 0) continue;
            replPeers.push_back({
                static_cast<rune::replication::NodeId>(peer.id),
                peer.replicationChannel()
            });
        }

        if (!replPeers.empty()) {
            const int32_t wc = config_.workerCount >= 1 ? config_.workerCount : 1;
            const std::string& inboundChannel = selfPeer.replicationChannel();
            if (inboundChannel.empty()) {
                spdlog::warn("BrokerNode[{}]: perWorkerReplication=true but self has no "
                             "replicationPort — per-worker transports skipped",
                             config_.nodeId);
            } else {
                perWorkerReplTransports_.reserve(static_cast<std::size_t>(wc));
                for (int32_t w = 0; w < wc; ++w) {
                    const int32_t streamId = config_.replBaseStreamId + w;
                    auto t = std::make_unique<WorkerReplicationTransport>(
                        aeron_, w, inboundChannel, streamId, replPeers, streamId);
                    if (!t->resolved()) {
                        // Hard-refuse: partial plane = silent message loss for the
                        // partitions assigned to worker w.
                        throw std::runtime_error(
                            "BrokerNode[" + std::to_string(config_.nodeId) +
                            "]: per-worker replication transport for worker " +
                            std::to_string(w) + " did not resolve within 5s "
                            "(stream=" + std::to_string(streamId) + ") — "
                            "refusing to start with a half-wired plane");
                    }
                    perWorkerReplTransports_.push_back(std::move(t));
                }
                spdlog::info("BrokerNode[{}]: {} per-worker replication transport(s) built "
                             "(replBaseStreamId={}, {} peer(s))",
                             config_.nodeId, wc, config_.replBaseStreamId,
                             replPeers.size());
            }
        } else {
            spdlog::info("BrokerNode[{}]: perWorkerReplication=true but no replication peers — "
                         "per-worker planes skipped (single-node or no peer repl ports)",
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
    // Task 10 round 4 (Critical 1): the legacy replication transport resolves
    // every inbound frame through the registry at USE time and holds no replica
    // pointer (a removed partition can never be reached through it).  User
    // replicas on per-worker planes stay off it (E5 Task 5), as before.
    if (replTransport_)
        replTransport_->setReplicaRegistry(replicaRegistry_.get(),
                                           /*userPartitions=*/perWorkerReplTransports_.empty());

    // E5 Task 5: build a local ShardRouter for per-worker transport selection
    // when per-worker planes are available.  Mirrors buildWorkerPool() exactly
    // so the transport assignment matches the worker's owned shard set.
    // Only constructed when we have planes to route to.
    std::optional<rune::metadata::ShardRouter> replRouter;
    if (!perWorkerReplTransports_.empty()) {
        const int32_t wc = static_cast<int32_t>(perWorkerReplTransports_.size());
        std::set<std::string> groups;
        for (const auto& ts : config_.topics)
            if (!ts.coPartitionGroup.empty()) groups.insert(ts.coPartitionGroup);
        std::unordered_map<std::string, int32_t> groupKey;
        int32_t nextGk = -1;
        for (const auto& g : groups) groupKey[g] = nextGk--;
        rune::metadata::ShardRouter sr(wc);
        for (const auto& ts : config_.topics) {
            if (!ts.coPartitionGroup.empty())
                sr.mapTopicToGroupKey(ts.topicId, groupKey[ts.coPartitionGroup]);
        }
        replRouter = std::move(sr);
    }

    const int32_t topicCount = topicRegistry_->topicCount();
    for (int32_t topicId = 0; topicId < topicCount; ++topicId) {
        const auto* spec = topicRegistry_->topic(topicId);
        if (!spec) continue;

        for (int32_t part = 0; part < spec->partitionCount; ++part) {
            // Task M1: select log backend by persistence mode.
            // Memory-backed: create an InMemoryLog (large caps — production-suitable).
            // Disk-backed: use the Log from TopicRegistry (recovered on startup).
            // De-virtualized seam (Milestone D P2): keep the concrete typed pointer
            // and construct via the matching PartitionReplica overload — no ILog*.
            rune::storage::Log*         diskLogPtr = nullptr;
            rune::storage::InMemoryLog* memLogPtr  = nullptr;
            const bool isMemory = (spec->persistence == rune::node::PersistenceMode::Memory);

            if (isMemory) {
                // Memory-backed partition: create an InMemoryLog.
                // Recovery SKIPS memory partitions — they start empty (precondition #3).
                // Use config_.memoryLogRingBytes (default 8 MiB, env RUNE_MEMORY_LOG_RING_BYTES)
                // as the fixed circular-ring capacity.  Keep DropOldest (not BackPressure)
                // so existing memory-persistence tests are parity-unchanged.
                const std::size_t kMemLogMaxBytes =
                    static_cast<std::size_t>(config_.memoryLogRingBytes);
                constexpr std::size_t kMemLogMaxRecords = 1'000'000UL;
                memoryLogs_.push_back(
                    std::make_unique<rune::storage::InMemoryLog>(
                        kMemLogMaxBytes, kMemLogMaxRecords,
                        rune::storage::FullPolicy::DropOldest));
                memLogPtr = memoryLogs_.back().get();
                spdlog::debug("BrokerNode[{}]: topic '{}' partition {} using InMemoryLog",
                              config_.nodeId, spec->name, part);
            } else {
                diskLogPtr = topicRegistry_->log(topicId, part);
                if (!diskLogPtr) {
                    spdlog::warn("BrokerNode[{}]: log not found for topic {} partition {}; skipping replica",
                                 config_.nodeId, spec->name, part);
                    continue;
                }
            }

            PartitionConfig pcfg;
            pcfg.topicId              = topicId;   // DR-BUG-5: multi-topic routing
            pcfg.partitionId          = static_cast<PartitionId>(part);
            pcfg.selfId               = selfId;
            pcfg.replicaLagMaxMs      = config_.replicaLagMaxMs;  // GR-5: configurable
            pcfg.replicaLagMaxOffset  = 1'000'000'000;  // TIME-based lag only (see note at first use)
            pcfg.minIsr               = config_.gatewayMinIsr;    // M1: consistent with gateway gate
            pcfg.fetchIntervalNs      = config_.fetchIntervalNs;
            pcfg.isrCheckIntervalNs   = 10'000'000LL;
            pcfg.maxFetchBytes        = 1 << 20;
            // CP-2: thread TopicSpec.cleanupPolicy into PartitionConfig so that
            // runBoundedCompaction() reads the correct policy from every replica.
            // TopicSpec::CleanupPolicy and rune::replication::CleanupPolicy share
            // the same underlying values (both uint8_t, 0=Delete/1=Compact/2=CompactDelete).
            pcfg.cleanupPolicy = static_cast<rune::replication::CleanupPolicy>(
                static_cast<uint8_t>(spec->cleanupPolicy));
            // CP-2: compaction tuning — thread from TopicSpec (with sane defaults).
            pcfg.minCleanableDirtyRatio = spec->minCleanableDirtyRatio;
            pcfg.deleteRetentionMs      = spec->deleteRetentionMs;
            // CP-3: size/time retention — thread from TopicSpec (-1 = unbounded).
            pcfg.retentionBytes         = spec->retentionBytes;
            pcfg.retentionMs            = spec->retentionMs;
            // CP-8: compaction lag gates — thread from TopicSpec.
            pcfg.minCompactionLagMs     = spec->minCompactionLagMs;
            pcfg.maxCompactionLagMs     = spec->maxCompactionLagMs;
            // CP-8: per-topic segment roll bytes for coalescing group bound.
            pcfg.segmentRollBytes       = spec->segmentRollBytes;
            // CP-9: retention scheduler cadence + async-delete delay.
            // These were missing from BrokerNode::buildReplication (product bug
            // exposed by the CP-5.1 smoke suite) — fixes retention not firing
            // at the configured cadence when retentionCheckMs/fileDeleteDelayMs
            // are set to small values in tests.
            pcfg.retentionCheckMs       = spec->retentionCheckMs;
            pcfg.fileDeleteDelayMs      = spec->fileDeleteDelayMs;
            pcfg.logStartOffset         = spec->logStartOffset;
            // Task M1 / precondition #1: forward parsed durability into PartitionConfig.
            // 0 = Majority (default), 1 = AllInSync.
            pcfg.durability = static_cast<rune::replication::DurabilityMode>(spec->durability);
            // E3 Task 5: forward async-durability tier (0=Leader, 1=Majority, 2=Strict).
            pcfg.durabilityTier = spec->durabilityTier;

            // Determine the transport:
            //
            //   E5 Task 5 routing rules:
            //   1. topicId < 0 (internal: __consumer_offsets=-1, __transaction_state=-2,
            //      __cluster_metadata=-3, __topic_schemas=-4):
            //      ALWAYS the legacy replTransport_ regardless of flag.
            //      Rationale: per-worker planes are worker-thread-only; internal
            //      partitions are driven on the control thread by the bridge/services.
            //
            //   2. topicId >= 0 AND per-worker planes were built:
            //      Use the plane for the worker that owns this (topicId, partition).
            //      Transport selection is pure (deterministic ShardRouter hash) and
            //      matches the worker's owned shard set exactly.
            //
            //   3. Flag off, or no planes built (no peers / no aeron_):
            //      Legacy replTransport_ exactly as before (flag-off path unchanged).
            PartitionTransport* transport = nullptr;
            if (topicId >= 0 && !perWorkerReplTransports_.empty() && replRouter) {
                // E5 Task 5: per-worker transport for user partitions.
                const int32_t workerIdx = replRouter->workerForPartition(topicId, part);
                transport = perWorkerReplTransports_[static_cast<std::size_t>(workerIdx)].get();
            } else {
                // Legacy path: real Aeron transport if available, else no-op.
                transport = replTransport_.get();
                if (!transport) {
                    // Allocate a no-op InMemoryTransport owned by the replica registry.
                    // InMemoryTransport with no registered leader/follower drops all frames.
                    nullTransports_.push_back(std::make_unique<InMemoryTransport>());
                    transport = nullTransports_.back().get();
                }
            }

            // De-virtualized seam (Milestone D P2): dispatch to the matching
            // concrete constructor overload so the replica stores a typed pointer.
            auto replica = isMemory
                ? std::make_unique<PartitionReplica>(
                      pcfg, Role::Follower, /*epoch=*/0,
                      allNodeIds, allNodeIds, memLogPtr, transport)
                : std::make_unique<PartitionReplica>(
                      pcfg, Role::Follower, /*epoch=*/0,
                      allNodeIds, allNodeIds, diskLogPtr, transport);
            // Task 10 (spec §3.1): user replicas are driven by their shard worker,
            // which submits the follower flush to its io backend (never inline).
            if (config_.shardMode() && topicId >= 0) replica->setExternalFollowerFlush(true);

            if (replTransport_) {
                // DR-BUG-5: register with composite (topicId, partitionId) key so
                // replicas from different topics with the same partition index do not
                // collide in the transport routing table.
                //
                // E5 Task 5: when per-worker planes are live, user (topicId>=0) replicas
                // are routed through per-worker transports — do NOT register them with
                // the legacy transport's routing table (that would cause the control
                // thread's AeronPartitionTransport::poll to dispatch to them, which
                // violates shared-nothing: only the worker thread should drive them).
                // Internal (topicId<0) replicas ALWAYS stay on the legacy transport.
                const bool onPerWorkerPlane =
                    (topicId >= 0 && !perWorkerReplTransports_.empty());
                if (!onPerWorkerPlane) {
                    replTransport_->registerReplica(topicId, replica->partitionId(), replica.get());
                }
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
// BrokerNode::createRuntimeUserReplicas — Task 8b (RULING-4, SINGLE-NODE).
//
// Create + register the user PartitionReplica(s) for a RUNTIME-defined topic
// so it becomes produce/consume-capable.  Called on the CONTROL thread from the
// admin create-topic hook AFTER TopicRegistry::createTopic() has assigned the
// authoritative id and created the dirs/logs.  This is a faithful, single-topic
// replay of the boot buildReplication() user-topic loop — same PartitionConfig
// threading, same InMemoryTransport fallback, same registry + transport
// registration.  Adding to replicaRegistry_ bumps its generation, so the next
// NodeRuntimeAgent::doWork cycle's WorkerPool::reconcile() assigns the replica to
// a worker; addRuntimeTopic() on the MetadataService emits the assignment so the
// bridge promotes the replica to Leader (single-node this node is the leader).
// ---------------------------------------------------------------------------
int32_t BrokerNode::createRuntimeUserReplicas(const TopicSpec& spec) {
    using namespace rune::replication;

    if (spec.topicId < 0) return 0;             // internal topics not handled here
    if (!replicaRegistry_ || !topicRegistry_) return 0;

    const NodeId selfId = static_cast<NodeId>(config_.nodeId);
    std::vector<NodeId> allNodeIds;
    allNodeIds.reserve(config_.peers.size());
    for (const auto& peer : config_.peers)
        allNodeIds.push_back(static_cast<NodeId>(peer.id));
    if (allNodeIds.empty()) allNodeIds.push_back(selfId);  // single-node safety

    const int32_t topicId = spec.topicId;
    int32_t registered = 0;

    // MUST-FIX (final review, item a): route runtime-created USER topics
    // (topicId>=0) to the per-worker replication plane when planes are live —
    // mirrors buildReplication().  Without this a runtime topic would land on the
    // legacy transport (a different stream) while peers place it on a per-worker
    // plane → it would silently fail to replicate on a multi-node cluster.
    // Internal (topicId<0) always stays on legacy.  (ShardRouter build mirrors
    // buildReplication + buildWorkerPool; tech-debt: extract a shared helper.)
    std::optional<rune::metadata::ShardRouter> rtReplRouter;
    if (!perWorkerReplTransports_.empty()) {
        const int32_t wc = static_cast<int32_t>(perWorkerReplTransports_.size());
        std::set<std::string> groups;
        for (const auto& ts : config_.topics)
            if (!ts.coPartitionGroup.empty()) groups.insert(ts.coPartitionGroup);
        std::unordered_map<std::string, int32_t> groupKey;
        int32_t nextGk = -1;
        for (const auto& g : groups) groupKey[g] = nextGk--;
        rune::metadata::ShardRouter sr(wc);
        for (const auto& ts : config_.topics)
            if (!ts.coPartitionGroup.empty())
                sr.mapTopicToGroupKey(ts.topicId, groupKey[ts.coPartitionGroup]);
        rtReplRouter = std::move(sr);
    }

    for (int32_t part = 0; part < spec.partitionCount; ++part) {
        // Skip if this partition replica already exists (idempotent re-invoke).
        if (replicaRegistry_->lookup(topicId, part) != nullptr) continue;

        rune::storage::Log*         diskLogPtr = nullptr;
        rune::storage::InMemoryLog* memLogPtr  = nullptr;
        const bool isMemory =
            (spec.persistence == rune::node::PersistenceMode::Memory);

        if (isMemory) {
            const std::size_t kMemLogMaxBytes =
                static_cast<std::size_t>(config_.memoryLogRingBytes);
            constexpr std::size_t kMemLogMaxRecords = 1'000'000UL;
            memoryLogs_.push_back(std::make_unique<rune::storage::InMemoryLog>(
                kMemLogMaxBytes, kMemLogMaxRecords,
                rune::storage::FullPolicy::DropOldest));
            memLogPtr = memoryLogs_.back().get();
        } else {
            // Reuse the Log created + recovered by TopicRegistry::createTopic (Task 5).
            diskLogPtr = topicRegistry_->log(topicId, part);
            if (!diskLogPtr) {
                spdlog::warn("BrokerNode[{}]: runtime topic '{}' partition {} has no log; "
                             "skipping replica", config_.nodeId, spec.name, part);
                continue;
            }
        }

        // Mirror buildReplication()'s PartitionConfig threading exactly.
        PartitionConfig pcfg;
        pcfg.topicId              = topicId;
        pcfg.partitionId          = static_cast<PartitionId>(part);
        pcfg.selfId               = selfId;
        pcfg.replicaLagMaxMs      = config_.replicaLagMaxMs;
        pcfg.replicaLagMaxOffset  = 1'000'000'000;
        pcfg.minIsr               = config_.gatewayMinIsr;
        pcfg.fetchIntervalNs      = config_.fetchIntervalNs;
        pcfg.isrCheckIntervalNs   = 10'000'000LL;
        pcfg.maxFetchBytes        = 1 << 20;
        pcfg.cleanupPolicy = static_cast<rune::replication::CleanupPolicy>(
            static_cast<uint8_t>(spec.cleanupPolicy));
        pcfg.minCleanableDirtyRatio = spec.minCleanableDirtyRatio;
        pcfg.deleteRetentionMs      = spec.deleteRetentionMs;
        pcfg.retentionBytes         = spec.retentionBytes;
        pcfg.retentionMs            = spec.retentionMs;
        pcfg.minCompactionLagMs     = spec.minCompactionLagMs;
        pcfg.maxCompactionLagMs     = spec.maxCompactionLagMs;
        pcfg.segmentRollBytes       = spec.segmentRollBytes;
        pcfg.retentionCheckMs       = spec.retentionCheckMs;
        pcfg.fileDeleteDelayMs      = spec.fileDeleteDelayMs;
        pcfg.logStartOffset         = spec.logStartOffset;
        pcfg.durability = static_cast<rune::replication::DurabilityMode>(spec.durability);
        pcfg.durabilityTier = spec.durabilityTier;

        const bool onPerWorkerPlane =
            (topicId >= 0 && !perWorkerReplTransports_.empty() && rtReplRouter);
        PartitionTransport* transport = nullptr;
        if (onPerWorkerPlane) {
            const int32_t workerIdx = rtReplRouter->workerForPartition(topicId, part);
            transport = perWorkerReplTransports_[static_cast<std::size_t>(workerIdx)].get();
        } else {
            transport = replTransport_.get();
            if (!transport) {
                nullTransports_.push_back(std::make_unique<InMemoryTransport>());
                transport = nullTransports_.back().get();
            }
        }

        auto replica = isMemory
            ? std::make_unique<PartitionReplica>(
                  pcfg, Role::Follower, /*epoch=*/0,
                  allNodeIds, allNodeIds, memLogPtr, transport)
            : std::make_unique<PartitionReplica>(
                  pcfg, Role::Follower, /*epoch=*/0,
                  allNodeIds, allNodeIds, diskLogPtr, transport);
        // Task 10 (spec §3.1): shard-worker-driven → async follower flush.
        if (config_.shardMode() && topicId >= 0) replica->setExternalFollowerFlush(true);

        // User replicas on a per-worker plane are polled by their owning worker —
        // do NOT register them in the legacy transport's routing table (that would
        // double-dispatch via the control thread). Internal/legacy replicas: register.
        if (replTransport_ && !onPerWorkerPlane)
            replTransport_->registerReplica(topicId, replica->partitionId(), replica.get());

        // add() bumps ReplicaRegistry::generation() → WorkerPool::reconcile()
        // picks it up on the next control cycle and assigns it to a worker.
        replicaRegistry_->add(topicId, part, std::move(replica));
        ++registered;
    }

    // Register the runtime topic with the MetadataService so its assignment is
    // emitted (leader-only) and the bridge promotes the replica to Leader.
    // buildStaticRoutingImage() / future rebalances also pick it up.
    if (metadataService_) {
        rune::metadata::TopicSpec bs;
        bs.topicId          = topicId;
        bs.partitionCount   = spec.partitionCount;
        bs.rf               = static_cast<int16_t>(std::max<std::size_t>(1, allNodeIds.size()));
        bs.coPartitionGroup = spec.coPartitionGroup;
        bs.partitionKey     = spec.partitionKey;
        metadataService_->addRuntimeTopic(bs);
    }

    spdlog::info("BrokerNode[{}]: runtime topic '{}' (id={}) → {} user replica(s) registered "
                 "(single-node); assignment emitted via MetadataService={}",
                 config_.nodeId, spec.name, topicId, registered,
                 metadataService_ ? "yes" : "no");
    return registered;
}

// ---------------------------------------------------------------------------
// BrokerNode::buildWorkerPool — E1: create WorkerPool and assign replicas.
//
// Must be called AFTER replicaRegistry_ is fully populated (i.e. AFTER
// buildGateway() has added all user, offset, txn, and metadata replicas).
//
// Grouping rule: distinct non-empty TopicSpec::coPartitionGroup strings are
// sorted and mapped to negative group keys (-1, -2, …) in sorted order. This
// mirrors StaticBalancer's sorted-group-name determinism so all nodes derive
// identical shard identities. Ungrouped topics fall back to their topicId.
//
// Ordinal invariant: partitionId() == partition ordinal (pcfg.partitionId = part
// at BrokerNode.cpp:434). ShardRouter uses the ordinal as the partition index.
// The assert below documents + enforces this; if partitionId ever becomes a
// global gpid this will fire, preventing silent co-partition affinity breakage.
// ---------------------------------------------------------------------------
void BrokerNode::buildWorkerPool() {
    const int32_t wc = config_.workerCount >= 1 ? config_.workerCount : 1;

    // Assign a stable negative group key to each distinct non-empty
    // coPartitionGroup, in sorted group-name order (matches StaticBalancer
    // determinism so every node derives identical shard identities).
    std::set<std::string> groups;
    for (const auto& ts : config_.topics)
        if (!ts.coPartitionGroup.empty()) groups.insert(ts.coPartitionGroup);
    std::unordered_map<std::string, int32_t> groupKey;
    int32_t next = -1;
    for (const auto& g : groups) groupKey[g] = next--;

    rune::metadata::ShardRouter router(wc);
    for (const auto& ts : config_.topics) {
        if (!ts.coPartitionGroup.empty())
            router.mapTopicToGroupKey(ts.topicId, groupKey[ts.coPartitionGroup]);
        // ungrouped topics: router uses topicId as-is (ShardRouter default).
    }

    workerPool_ = std::make_unique<WorkerPool>(
        wc, std::move(router),
        static_cast<std::size_t>(config_.workerIngressRingBytes),  // NodeConfig-configurable (bytes, pow2)
        static_cast<std::size_t>(config_.workerReturnRingBytes),   // NodeConfig-configurable (bytes, pow2)
        static_cast<std::size_t>(config_.ioSubmitRingDepth),       // E3 Task 7: io-ring depths
        static_cast<std::size_t>(config_.ioCompletionRingDepth),   // E3 Task 7
        config_.workerCoreBase);                                    // E4: CPU affinity base core

    if (replicaRegistry_) {
        // Ordinal-invariant assert: routing uses partitionId() as the partition
        // ordinal. This holds because pcfg.partitionId = part (BrokerNode.cpp:434),
        // i.e. partitionId() == the 0-based partition ordinal within the topic.
        // If partitionId ever becomes a global gpid this assertion will fire,
        // preventing silent co-partition affinity routing breakage. Internal
        // system replicas (topicId < 0) use partitionId = 0 — also valid.
        for (auto* r : replicaRegistry_->allReplicas()) {
            if (!r) continue;
            assert(static_cast<int32_t>(r->partitionId()) >= 0 &&
                   "partitionId() must be a non-negative ordinal (pcfg.partitionId = part)");
        }
        workerPool_->assignFromRegistry(*replicaRegistry_);
        // Task 10 fix 3 (I2): ops are only routed after an inline reconcile, so
        // the SetOwnership that governs them is always ahead of them.
        workerPool_->setRegistry(replicaRegistry_.get());
    }

    // -----------------------------------------------------------------------
    // E5.1.3 (c): Per-worker Aeron ingress subscriptions.
    //
    // When RUNE_PER_WORKER_INGRESS=true, allocate one WorkerIngress per worker
    // on stream (workerIngressBaseStreamId + workerId) and wire it into the
    // worker via setWorkerIngress(ingress, /*active=*/true).
    //
    // DEFAULT: false — the gateway fanout remains the live production path.
    // The gateway produce fanout is gated on !perWorkerIngressEnabled (deliverable e).
    //
    // IMPORTANT: the per-worker ingress is only created here when aeron_ is set
    // (aeron_ is null in unit tests that do not start the embedded driver).
    // Tests that need the per-worker path construct WorkerIngress directly.
    // -----------------------------------------------------------------------

    // Apply desired fragment limit + ack-slot reservation size from NodeConfig to
    // each worker (before runners start).
    for (int32_t i = 0; i < wc; ++i) {
        workerPool_->worker(i).setDesiredFragmentLimit(config_.gatewayFragmentLimit);
        workerPool_->worker(i).setAckSlotBytes(config_.workerAckSlotBytes);
        workerPool_->worker(i).setIngressDrainLimit(config_.workerIngressDrainLimit);
        workerPool_->worker(i).setReturnDrainLimit(config_.workerReturnDrainLimit);
        workerPool_->worker(i).setMaxProduceBatchBytes(
            static_cast<std::size_t>(config_.produceIntakeLimit()));
    }

    if (config_.shardMode()) {
        // Spec §3.1 never-block: a shard has no inline-fsync fallback, so the
        // selected io backend MUST accept submissions (Task 10 fix 1 / C1).
        for (int32_t i = 0; i < wc; ++i) {
            if (!workerPool_->worker(i).ioAcceptsSubmissions()) {
                const char* v = std::getenv(kIoBackendEnv);
                throw std::runtime_error(
                    "BrokerNode[" + std::to_string(config_.nodeId) + "]: " + kIoBackendEnv + "=" +
                    (v ? v : "") + " selects an io backend that cannot accept submissions; shard "
                    "mode requires a working async-durability backend (unset it, or use 'portable'"
#if defined(__linux__)
                    " or 'uring'"
#endif
                    ") — refusing to start");
            }
        }
        // Self-contained worker shards (spec §3.1): each worker's client ingress,
        // record egress and plane are attached in attachShardRuntimes_() on its
        // OWN client, after the long startup phases (fix round 1 I2).
        for (int32_t i = 0; i < wc; ++i) workerPool_->worker(i).setShardMode(true);
        // Spec §3.3: Metadata advertises each partition's LEADER-worker ingress
        // endpoint, from explicit RUNE_PEERS configuration (never derived).  Set
        // here, before the control thread runs (single-writer, no sharing).
        if (gateway_) {
            std::vector<rune::gateway::ShardIngressEndpoint> eps;
            eps.reserve(config_.peers.size());
            for (const auto& peer : config_.peers)
                eps.push_back({peer.id, peer.host, static_cast<int32_t>(peer.ingressBasePort)});
            gateway_->setShardIngressEndpoints(std::move(eps));
        }
    } else {
        if (config_.perWorkerIngressEnabled && aeron_) {
            const std::string& channel  = config_.clientEndpoint.channel;
            const int32_t      baseStream = config_.workerIngressBaseStreamId;

            workerIngresses_.reserve(static_cast<std::size_t>(wc));
            for (int32_t i = 0; i < wc; ++i) {
                const int32_t streamId = baseStream + i;
                auto ing = std::make_unique<WorkerIngress>(aeron_, channel, streamId);
                if (!ing->resolved()) {
                    spdlog::warn("BrokerNode[{}]: WorkerIngress for worker {} on stream {} "
                                 "did not resolve within 5s — per-worker ingress disabled for this worker",
                                 config_.nodeId, i, streamId);
                } else {
                    spdlog::info("BrokerNode[{}]: WorkerIngress for worker {} wired on stream {} (channel={})",
                                 config_.nodeId, i, streamId, channel);
                }
                workerPool_->worker(i).setWorkerIngress(ing.get(), /*active=*/ing->resolved());
                workerIngresses_.push_back(std::move(ing));
            }
            // Gate the gateway produce fanout: when per-worker ingress is active,
            // produce arrives on each worker's dedicated stream — gateway must NOT also
            // enqueue to workerPool_ (that would double-process the same produce).
            if (gateway_) {
                gateway_->setPerWorkerIngressActive(true);
                spdlog::info("BrokerNode[{}]: gateway produce fanout GATED (perWorkerIngressActive=true)",
                             config_.nodeId);

                // E5.1.3 (d) R7 fix: register all user-partition logs as worker-owned
                // so flushDirtyLogs() skips them (the worker calls log->sync on its
                // own thread — the only thread appending to these logs).
                if (replicaRegistry_ && topicRegistry_) {
                    for (const auto& ts : config_.topics) {
                        for (int32_t p = 0; p < ts.partitionCount; ++p) {
                            rune::storage::Log* log = topicRegistry_->log(ts.topicId, p);
                            if (log) {
                                gateway_->addWorkerOwnedLog(log);
                            }
                        }
                    }
                    spdlog::info("BrokerNode[{}]: {} user-partition logs marked worker-owned (R7 fix)",
                                 config_.nodeId, config_.topics.size());
                }
            }
            spdlog::info("BrokerNode[{}]: per-worker ingress ACTIVE ({} subscriptions on base stream {})",
                         config_.nodeId, wc, baseStream);
        } else if (config_.perWorkerIngressEnabled && !aeron_) {
            spdlog::warn("BrokerNode[{}]: RUNE_PER_WORKER_INGRESS=true but aeron_ is null — "
                         "per-worker ingress NOT activated (no embedded driver)",
                         config_.nodeId);
        }

        // -----------------------------------------------------------------------
        // read-rework T3/T4 (I1b/I3b): per-worker dynamic-MDC EGRESS publications.
        //
        // Independent of per-worker ingress.  Each worker owns ONE ExclusivePublication
        // it alone writes; consumers self-register via control-mode=dynamic.  Control
        // address = host:(basePort+W), streamId = egressBase + W.  egressBase defaults
        // to workerIngressBaseStreamId + workerCount so the ingress/egress stream ranges
        // never overlap.  When active, the gateway routes Subscribe/Unsubscribe to the
        // owning worker and stops pushing user partitions itself (I3b).
        // -----------------------------------------------------------------------
        if (config_.perWorkerEgressEnabled && aeron_) {
            int32_t egressBase = config_.workerEgressBaseStreamId;
            if (egressBase <= 0) egressBase = config_.workerIngressBaseStreamId + wc;
            workerEgresses_.reserve(static_cast<std::size_t>(wc));
            for (int32_t i = 0; i < wc; ++i) {
                const int32_t     egStream = egressBase + i;
                const std::string control  = config_.workerEgressControlHost + ":" +
                    std::to_string(config_.effectiveWorkerEgressControlBasePort() + i);
                auto eg = std::make_unique<WorkerEgress>(aeron_, control, egStream);
                if (!eg->resolved()) {
                    spdlog::warn("BrokerNode[{}]: WorkerEgress for worker {} on stream {} "
                                 "control {} did not resolve within 5s — per-worker egress "
                                 "disabled for this worker",
                                 config_.nodeId, i, egStream, control);
                } else {
                    spdlog::info("BrokerNode[{}]: WorkerEgress for worker {} wired on stream {} "
                                 "(dynamic-MDC control={})",
                                 config_.nodeId, i, egStream, control);
                }
                workerPool_->worker(i).setWorkerEgress(eg.get());
                workerEgresses_.push_back(std::move(eg));
            }
            if (gateway_) {
                gateway_->setPerWorkerEgressActive(true);
                gateway_->setWorkerEgressParams(config_.effectiveWorkerEgressControlBasePort(), egressBase);
                spdlog::info("BrokerNode[{}]: per-worker EGRESS ACTIVE — gateway routes "
                             "Subscribe/Unsubscribe to owning workers (base stream {}, "
                             "control base port {})",
                             config_.nodeId, egressBase, config_.effectiveWorkerEgressControlBasePort());
            }
        } else if (config_.perWorkerEgressEnabled && !aeron_) {
            spdlog::warn("BrokerNode[{}]: RUNE_PER_WORKER_EGRESS=true but aeron_ is null — "
                         "per-worker egress NOT activated (no embedded driver)",
                         config_.nodeId);
        }
    }

    // -----------------------------------------------------------------------
    // E5 Task 5: wire each worker's per-worker replication transport.
    //
    // perWorkerReplTransports_ is populated by buildReplication() when
    // perWorkerReplication=true && aeron_ && there are replication peers.
    // At this point the workers' AgentRunners have NOT yet started, so it is
    // safe to call setReplicationTransport (startup / single-threaded path).
    //
    // Non-owning: BrokerNode owns the WorkerReplicationTransport lifetime;
    // workers hold raw pointers.  Task 6 will drive poll() from doWork;
    // for this task the pointer is just stored on the worker.
    // -----------------------------------------------------------------------
    if (!perWorkerReplTransports_.empty()) {
        const std::size_t planeCount = perWorkerReplTransports_.size();
        for (int32_t i = 0; i < wc; ++i) {
            const std::size_t idx = static_cast<std::size_t>(i) % planeCount;
            workerPool_->worker(i).setReplicationTransport(
                perWorkerReplTransports_[idx].get());
        }
        spdlog::info("BrokerNode[{}]: per-worker replication transport wired to {} worker(s)",
                     config_.nodeId, wc);
    }

    spdlog::info("BrokerNode[{}]: WorkerPool built ({} workers, {} replicas assigned)",
                 config_.nodeId, wc,
                 replicaRegistry_ ? replicaRegistry_->replicaCount() : std::size_t(0));
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
void BrokerNode::onStorageFailure_(const std::string& logDir, int err) {
    if (haltRaised_) return;
    haltRaised_ = true;
    spdlog::critical("BrokerNode[{}]: STORAGE FAILURE — fsync failed on {} (errno={}: {}). "
                     "The log is permanently failed; HALTING the broker (exit {}). Restart "
                     "recovery truncates to the last valid batch; replication re-heals it.",
                     config_.nodeId, logDir, err, std::strerror(err), kExitStorageFailure);
    if (haltHandler_) {
        haltHandler_(haltCtx_, kExitStorageFailure);
        return;
    }
    // No owner-installed handler: fail-stop immediately.  Restart recovery
    // truncates the torn tail; nothing on this node may run past a failed fsync.
    spdlog::critical("BrokerNode[{}]: no halt handler installed — exiting {} now",
                     config_.nodeId, kExitStorageFailure);
    spdlog::default_logger()->flush();
    std::_Exit(kExitStorageFailure);
}

void BrokerNode::onInvariantViolation_(const std::string& what) {
    if (haltRaised_) return;
    haltRaised_ = true;
    spdlog::critical("BrokerNode[{}]: INVARIANT VIOLATION — {} — halting (exit {})",
                     config_.nodeId, what, kExitInvariantViolation);
    if (haltHandler_) { haltHandler_(haltCtx_, kExitInvariantViolation); return; }
    spdlog::default_logger()->flush();
    std::_Exit(kExitInvariantViolation);
}

void BrokerNode::refuseStartOnFailedRecovery_() {
    // Every log this node will serve — registry replicas AND the logs outside
    // the registry (schema; the internal offset / txn / metadata logs; every
    // configured topic's partition logs).  Enumerated explicitly (Task 10 fix 3
    // / I1): the registry is not assumed to be complete.
    std::vector<rune::storage::Log*> logs;
    if (replicaRegistry_)
        for (auto* r : replicaRegistry_->allReplicas())
            if (r && r->isDiskBacked()) logs.push_back(r->log());
    for (auto* l : {schemaLog_.get(), offsetLog_.get(), txnLog_.get(), metadataLog_.get()})
        if (l) logs.push_back(l);
    if (topicRegistry_)
        for (const auto& ts : config_.topics)
            for (int32_t p = 0; p < ts.partitionCount; ++p)
                if (auto* l = topicRegistry_->log(ts.topicId, p)) logs.push_back(l);
    if (schemaRecoveryFaultForTest_ != 0 && schemaLog_) {
        schemaLog_->setSyncFaultForTest(schemaRecoveryFaultForTest_);
        schemaLog_->recover();
        schemaLog_->setSyncFaultForTest(0);
    }
    for (auto* l : logs) {
        if (!l->storageFailed()) continue;
        spdlog::critical("BrokerNode[{}]: log {} FAILED recovery (errno={}) — refusing to start",
                         config_.nodeId, l->dir(), l->storageErrno());
        onStorageFailure_(l->dir(), l->storageErrno());
        throw std::runtime_error("BrokerNode: log " + l->dir() + " failed recovery (storage failure) — refusing to start");
    }
    if (!replicaRegistry_) return;
    if (recoveryFaultForTest_ != 0) {
        // Test-only: re-run the first disk-backed user log's recovery with an
        // injected msync fault — the real recover() failure path.
        for (auto* r : replicaRegistry_->allReplicas()) {
            if (r && r->topicId() >= 0 && r->isDiskBacked()) {
                r->log()->setSyncFaultForTest(recoveryFaultForTest_);
                r->log()->recover();
                r->log()->setSyncFaultForTest(0);
                break;
            }
        }
    }
    for (auto* r : replicaRegistry_->allReplicas()) {
        if (!r || !r->isDiskBacked() || !r->log()->storageFailed()) continue;
        const std::string dir = r->log()->dir();
        const int err = r->log()->storageErrno();
        spdlog::critical("BrokerNode[{}]: log {} FAILED recovery (errno={}) — refusing to start",
                         config_.nodeId, dir, err);
        onStorageFailure_(dir, err);   // halt path: handler (exit 74) or _Exit(74)
        throw std::runtime_error("BrokerNode: log " + dir + " failed recovery (storage failure) — refusing to start");
    }
}

void BrokerNode::buildGateway() {
    if (startupPhaseHookForTest_) startupPhaseHookForTest_("build-gateway");   // test-only fault injection
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
        // fsyncgate fail-stop: worker StorageFailure signals (and the gateway's
        // own flushDirtyLogs) land here on the runtime thread → halt.
        gateway_->setStorageFailureHandler([this](const std::string& dir, int err) {
            onStorageFailure_(dir, err);
        });
        gateway_->setInvariantViolationHandler([this](const std::string& what) {
            onInvariantViolation_(what);
        });

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
        gateway_->setMaxProduceBatchBytes(
            static_cast<std::size_t>(config_.produceIntakeLimit()));
        spdlog::info("BrokerNode[{}]: produce intake limit {} B (frame ceiling {} B)", nodeId,
                     config_.produceIntakeLimit(), config_.produceBatchFrameCeiling());
        spdlog::info("BrokerNode[{}]: gateway minIsr set to {} (cluster size={})",
                     nodeId, config_.gatewayMinIsr, config_.peers.size());

        // G0.3: Apply the configurable fragment poll limit (RUNE_GATEWAY_FRAGMENT_LIMIT).
        // Default 256 (raised from hard-coded 10 to avoid head-of-line blocking on
        // batched ingress); sourced from NodeConfig::gatewayFragmentLimit.
        gateway_->setFragmentLimit(config_.gatewayFragmentLimit);
        spdlog::info("BrokerNode[{}]: gateway fragmentLimit set to {}",
                     nodeId, config_.gatewayFragmentLimit);

        // E5.1: Apply per-worker ingress base stream id (RUNE_WORKER_INGRESS_BASE_STREAM).
        // Each PartitionEntry in the MetadataResponse will carry
        //   ingressStreamId = base + ShardRouter.workerForPartition(topicId, partition)
        // so the client can connect directly to the owning worker's ingress stream.
        gateway_->setWorkerIngressBaseStreamId(config_.workerIngressBaseStreamId);
        spdlog::info("BrokerNode[{}]: workerIngressBaseStreamId set to {}",
                     nodeId, config_.workerIngressBaseStreamId);

        // MD1 Task 9: Build nodeId → client endpoint map for per-partition REDIRECT.
        // Built from NodeConfig::peers: each peer carries a clientPort (GB-4 per-peer
        // client endpoint).  A zero clientPort means no known client endpoint (empty
        // string); the gateway will fall back to NotLeader in that case.
        {
            std::unordered_map<int32_t, std::string> nodeEndpoints;
            nodeEndpoints.reserve(config_.peers.size());
            for (const auto& peer : config_.peers) {
                // Advertise EVERY node's real client endpoint — including self.
                // This map feeds two consumers: (1) the per-partition REDIRECT
                // path, which never looks up self (both redirect sites guard on
                // leaderNodeId != thisNodeId_ before the lookup), and (2) the
                // MetadataResponse broker list, which MUST include self so a
                // client seeded on this node can route to partitions THIS node
                // leads.  Blanking self broke (2): the seed node advertised its
                // own endpoint as "" → clients hit "endpoint unreachable" for
                // every partition led by the seed.
                nodeEndpoints[peer.id] = peer.clientChannel();
            }
            gateway_->setNodeEndpointMap(std::move(nodeEndpoints));
            spdlog::info("BrokerNode[{}]: gateway nodeEndpointMap wired ({} entries)",
                         nodeId, config_.peers.size());
        }
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
            offsetLog_ = std::make_unique<rune::storage::Log>(
                offsetDir.string(), config_.offsetLogSegmentRollBytes);
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
            ocfg.replicaLagMaxOffset  = 1'000'000'000;  // TIME-based lag only (see note at first use)
            ocfg.fetchIntervalNs      = config_.fetchIntervalNs;
            ocfg.isrCheckIntervalNs   = 10'000'000LL;
            ocfg.maxFetchBytes        = 1 << 20;
            // CP-1: __consumer_offsets is a compacted topic (keep latest per key).
            ocfg.cleanupPolicy        = rune::replication::CleanupPolicy::Compact;
            // CP-2: compaction tuning for __consumer_offsets sentinel topic.
            ocfg.minCleanableDirtyRatio = 0.5;          // 50% dirty before compaction fires
            ocfg.deleteRetentionMs      = 86'400'000LL; // 1 day tombstone retention
            // CP-10 Fix 3: match segmentRollBytes to the offset log's actual roll so
            // the coalescer group bound is consistent with what the cleaner uses per-topic.
            ocfg.segmentRollBytes       = config_.offsetLogSegmentRollBytes;

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
            tcfg.replicaLagMaxOffset  = 1'000'000'000;  // TIME-based lag only (see note at first use)
            tcfg.fetchIntervalNs      = config_.fetchIntervalNs;
            tcfg.isrCheckIntervalNs   = 10'000'000LL;
            tcfg.maxFetchBytes        = 1 << 20;
            // CP-1: __transaction_state is a compacted topic (keep latest per txnId).
            tcfg.cleanupPolicy        = rune::replication::CleanupPolicy::Compact;
            // CP-2: compaction tuning for __transaction_state sentinel topic.
            tcfg.minCleanableDirtyRatio = 0.5;          // 50% dirty before compaction fires
            tcfg.deleteRetentionMs      = 86'400'000LL; // 1 day tombstone retention
            // CP-10 Fix 3: __transaction_state uses the same roll size as the offset log
            // (offsetLogSegmentRollBytes) so the coalescer group bound is consistent.
            tcfg.segmentRollBytes       = config_.offsetLogSegmentRollBytes;

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

    // -----------------------------------------------------------------------
    // MD1 Task 4: Build MetadataService + replicated __cluster_metadata log.
    //
    // Mirrors the GroupCoordinationService / TransactionCoordinationService
    // wiring above.  topicId=-3 is the sentinel for __cluster_metadata
    // (offsets=-1, txn=-2).
    //
    // FINDING-1: the metadata partition's OWN leadership follows
    // cluster-consensus leadership, NOT the assignment map.  We register
    // metadataReplica_ in replicaRegistry_ so the existing
    // ReplicationMetadataBridge drives its leader/follower role from cluster
    // leadership automatically.  Do NOT try to make the metadata partition
    // follow the assignment map.
    //
    // Destruction order (reverse of construction, in stop()):
    //   runtimeAgent_ → metadataService_ → metadataReplica_ (in registry)
    //   → metadataLog_ → …
    // -----------------------------------------------------------------------
    // E2 shared-nothing fix (Part 2): MetadataService is now created
    // unconditionally (no longer guarded by `if (gateway_)`).
    //
    // Previously this guard prevented single-node user-partition promotion:
    // without gateway_, MetadataService was never wired, so the bridge's
    // applyUserPartitionRoles() never fired, and user partitions stayed
    // Follower → compaction/retention skipped them (LogCleanerAgent_CompactsViaBrokerNode
    // + CoalescingEndToEndViaBrokerNode both failed for this reason).
    //
    // This is now safe because Part 1 routes all becomeLeader/becomeFollower
    // calls from the bridge to the owning worker's SPSC ingress — eliminating
    // the control-thread vs worker-thread data race that the guard was implicitly
    // protecting against.
    {
        try {
            const fs::path metaDir = fs::path{config_.dataDir} / "__cluster_metadata";
            fs::create_directories(metaDir);

            metadataLog_ = std::make_unique<rune::storage::Log>(metaDir.string());
            metadataLog_->recover();

            const NodeId selfId = static_cast<NodeId>(config_.nodeId);

            std::vector<NodeId> allNodeIds;
            allNodeIds.reserve(config_.peers.size());
            for (const auto& peer : config_.peers) {
                allNodeIds.push_back(static_cast<NodeId>(peer.id));
            }

            rune::replication::PartitionConfig mcfg;
            mcfg.topicId              = -3;   // sentinel: __cluster_metadata
            mcfg.partitionId          = 0;
            mcfg.selfId               = selfId;
            // minIsr=1: metadata records are leader-local in Phase 1 (same as
            // offsets).  A later task may raise this once multi-node assignment
            // is wired end-to-end.
            mcfg.minIsr               = 1;
            mcfg.replicaLagMaxMs      = config_.replicaLagMaxMs;
            mcfg.replicaLagMaxOffset  = 1'000'000'000;  // TIME-based lag only (see note at first use)
            mcfg.fetchIntervalNs      = config_.fetchIntervalNs;
            mcfg.isrCheckIntervalNs   = 10'000'000LL;
            mcfg.maxFetchBytes        = 1 << 20;
            // __cluster_metadata: compacted log (keep latest assignment per key).
            mcfg.cleanupPolicy        = rune::replication::CleanupPolicy::Compact;
            mcfg.minCleanableDirtyRatio = 0.5;
            mcfg.deleteRetentionMs      = 86'400'000LL; // 1 day tombstone retention
            mcfg.segmentRollBytes       = config_.offsetLogSegmentRollBytes;

            // Transport: real Aeron transport if available, else no-op InMemoryTransport.
            rune::replication::PartitionTransport* metaTransport = replTransport_.get();
            if (!metaTransport) {
                nullTransports_.push_back(
                    std::make_unique<rune::replication::InMemoryTransport>());
                metaTransport = nullTransports_.back().get();
            }

            metadataReplica_ = std::make_unique<rune::replication::PartitionReplica>(
                mcfg,
                rune::replication::Role::Follower, // bridge promotes on first doWork
                /*epoch=*/0,
                allNodeIds,
                allNodeIds,
                metadataLog_.get(),
                metaTransport);

            // Register with Aeron transport (topicId=-3, partitionId=0).
            if (replTransport_) {
                replTransport_->registerReplica(-3, 0, metadataReplica_.get());
            }

            // Add to replicaRegistry_ so the metadata bridge drives role
            // transitions and NodeRuntimeAgent::doWork calls replica->doWork.
            if (replicaRegistry_) {
                replicaRegistry_->add(-3, 0, std::move(metadataReplica_));
                metadataReplica_.reset(); // ownership transferred to registry
            }

            // Re-acquire raw pointer for MetadataService construction.
            rune::replication::PartitionReplica* metaReplicaRaw =
                replicaRegistry_ ? replicaRegistry_->lookup(-3, 0) : nullptr;

            // MetadataService: applies committed records on doWork; exposes assignment().
            metadataService_ = std::make_unique<MetadataService>(
                metaReplicaRaw, nodeId);
            // Rebuild any previously committed assignments from the local Log.
            metadataService_->rebuildFromLog();

            // MD1 Task 7: wire the MSM into the bridge so USER partitions are
            // driven per-partition from the committed assignment map.
            // FINDING-1: internal partitions continue to follow cluster-consensus
            // leadership (the bridge handles that in Step A of doWork).
            if (replBridge_) {
                replBridge_->setMsm(&metadataService_->msmForTest());
            }

            // MD1 Task 6: configure static assignment emit context.
            // Convert NodeConfig::TopicSpec (broker config) to
            // rune::metadata::TopicSpec (balancer input).
            // rf defaults to cluster size (all nodes replicate every partition).
            // Task M1 Note C fix: coPartitionGroup + partitionKey are now forwarded
            // from the parsed TopicSpec (previously hard-coded empty strings).
            {
                std::vector<rune::metadata::TopicSpec> balancerTopics;
                balancerTopics.reserve(config_.topics.size());
                const auto rf = static_cast<int16_t>(
                    std::max(1, static_cast<int>(allNodeIds.size())));
                for (const auto& ts : config_.topics) {
                    if (ts.topicId < 0) continue; // skip internal topics
                    rune::metadata::TopicSpec bs;
                    bs.topicId          = ts.topicId;
                    bs.partitionCount   = ts.partitionCount;
                    bs.rf               = rf;
                    bs.coPartitionGroup = ts.coPartitionGroup; // Note C fix: from parsed config
                    bs.partitionKey     = ts.partitionKey;     // Note C fix: from parsed config
                    balancerTopics.push_back(std::move(bs));
                }
                // Convert allNodeIds (vector<NodeId>) to vector<int32_t> for balancer.
                std::vector<rune::metadata::NodeId> balancerNodes;
                balancerNodes.reserve(allNodeIds.size());
                for (const auto nid : allNodeIds) {
                    balancerNodes.push_back(static_cast<rune::metadata::NodeId>(nid));
                }
                metadataService_->configure(balancerTopics, balancerNodes);
                // Commit-quorum failover floor: a partition failover must never
                // install an ISR below the same minIsr the gateway enforces for
                // acks=all — otherwise a transiently-isolated controller seizes
                // leadership with ISR<minIsr and every produce is rejected (startup
                // collapse).  See DynamicBalancerInputs::minIsr.
                metadataService_->setMinIsr(config_.gatewayMinIsr);
                spdlog::info(
                    "BrokerNode[{}]: MetadataService configured for assignment emit "
                    "({} user topics, {} nodes, minIsr={})",
                    nodeId, balancerTopics.size(), balancerNodes.size(),
                    config_.gatewayMinIsr);
            }

            spdlog::info("BrokerNode[{}]: MetadataService wired (__cluster_metadata at {})",
                         nodeId, metaDir.string());
        } catch (const std::exception& ex) {
            spdlog::error("BrokerNode[{}]: MetadataService setup failed: {}; "
                          "cluster metadata will not be tracked", nodeId, ex.what());
            metadataService_.reset();
            metadataReplica_.reset();
            metadataLog_.reset();
        }
    }

    // E1: Build the WorkerPool + assign replicas to workers.
    // Must run AFTER replicaRegistry_ is fully populated (user topics, offset,
    // txn, and metadata replicas all added). The pool holds non-owning ptrs.
    buildWorkerPool();

    // E2 inc2a: Wire the WorkerPool into the gateway and the replication
    // transport so their per-partition log work is routed through the owning
    // worker's SPSC ingress ring.  Both setters are null-safe (no-op when
    // workerPool_ is null, which happens when no topics are configured).
    if (gateway_ && workerPool_) {
        gateway_->setWorkerPool(workerPool_.get());
    }
    // Round 5: an incarnation wrap must skip values the F7 owner map still holds.
    if (workerPool_ && replicaRegistry_)
        replicaRegistry_->setIncarnationInUseHook(&WorkerPool::tracksIncarnationThunk, workerPool_.get());
    if (replTransport_ && workerPool_) {
        replTransport_->setWorkerPool(workerPool_.get());
    }
    // E2 shared-nothing fix (Part 1): route replica role changes (becomeLeader/
    // becomeFollower) to the owning worker's SPSC ingress so they execute on the
    // worker thread — eliminating the control-thread vs worker-thread data race
    // confirmed by TSan (bridge control thread ↔ Worker::doWork on worker thread).
    if (replBridge_ && workerPool_) {
        replBridge_->setWorkerPool(workerPool_.get());
    }

    // -----------------------------------------------------------------------
    // Task 6: Build __topic_schemas log/replica + AdminWorker thread.
    // Must run AFTER buildWorkerPool() so AdminDeps.pushValidators can enqueue
    // SchemaUpdate ops to the worker pool.
    // -----------------------------------------------------------------------
    buildAdminWorker();

    // Task 8a (RULING-2 F): wire the gateway → admin request sink so DefineTopic
    // (64) / GetTopicSchema (66) frames are handed off to the AdminWorker ring
    // (no registry access on the gateway hot path).
    if (gateway_ && adminWorkerAgent_) {
        gateway_->setAdminSink(adminWorkerAgent_.get());
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
        txnCoordService_.get(),   // TX-3: txn coordinator (null-safe if setup failed)
        metadataService_.get(),   // MD1: metadata service (null-safe if setup failed)
        workerPool_.get());       // E1: shard→worker pool (null-safe if no topics)
    // fsyncgate (Task 10 fix 2 / I-1): control-thread-driven replicas (internal
    // partitions) whose fsync failed → halt.
    runtimeAgent_->setStorageFailureHook([this](const std::string& dir, int err) {
        onStorageFailure_(dir, err);
    });
    // F7 ownership invariant violation (a runtime replica move) → halt.
    runtimeAgent_->setFatalHook([this](const std::string& what) { onInvariantViolation_(what); });

    // -----------------------------------------------------------------------
    // Task 8a (RULING-2 B): wire the admin control-plane servicing hook.
    //
    // Runs on the CONTROL (duty-cycle) thread every cycle.  It owns the three
    // control-side ring endpoints for the schema DDL plane:
    //   1. Post a role event to the admin thread whenever cluster leadership for
    //      this node changes (Follower↔Leader) — the admin thread applies
    //      becomeLeader/becomeFollower on the schema replica ON ITS OWN THREAD.
    //   2. Drain createReqRing (admin → control): create the physical topic via
    //      the control-thread-owned TopicRegistry (the SAME mechanism the broker
    //      uses to make topics visible to the produce path) and reply the id.
    //   3. Drain egressRing (admin → control): send DDL response frames on the
    //      originating session's egress via gateway_->sendControlFrame.
    // No cross-thread mutation: TopicRegistry + gateway egress are touched ONLY
    // here on the control thread; the admin thread touches ONLY its replica.
    if (adminWorkerAgent_) {
        runtimeAgent_->setAdminControlHook(
            [this](int64_t /*nowNs*/) -> int {
                int work = 0;
                auto* agent = adminWorkerAgent_.get();
                if (!agent) return 0;

                // (1) Post a role event on a leadership-state change.
                const bool leaderNow = this->isLeader();
                const int32_t epochNow = static_cast<int32_t>(this->leadershipTermId());
                if (leaderNow != adminLastLeader_ || epochNow != adminLastEpoch_) {
                    AdminRoleMsg rm;
                    rm.leader   = leaderNow;
                    rm.epoch    = epochNow;
                    rm.leaderId = static_cast<int32_t>(this->leaderId());
                    if (encodeAdminRole(agent->roleRing(), rm)) {
                        adminLastLeader_ = leaderNow;
                        adminLastEpoch_  = epochNow;
                        ++work;
                    }
                    // else: ring full (impossible for 1-deep role churn) → retry next cycle
                }

                // (1b) Task 10 fix 3 / I1: schema-log storage failure (admin →
                //      control ring) → the same halt path as every other log.
                work += agent->failureRing().drain(
                    [this](int32_t /*tid*/, aeron::concurrent::AtomicBuffer& b,
                           aeron::util::index_t off, aeron::util::index_t len) {
                        const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
                        int32_t e = 0;
                        std::memcpy(&e, p, 4);
                        onStorageFailure_(std::string(reinterpret_cast<const char*>(p) + 4,
                                                      static_cast<std::size_t>(len) - 4), e);
                    }, 4);

                // (2) Drain create-topic requests (admin → control).
                work += agent->createReqRing().drain(
                    [this, agent](int32_t /*tid*/,
                                  aeron::concurrent::AtomicBuffer& b,
                                  aeron::util::index_t off,
                                  aeron::util::index_t len) {
                        const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
                        CreateTopicReqMsg req =
                            decodeCreateTopicReq(p, static_cast<std::size_t>(len));
                        int32_t id = -1;
                        if (topicRegistry_) {
                            rune::node::TopicSpec spec;
                            spec.name           = req.name;
                            spec.partitionCount = req.partitionCount;
                            spec.topicId        = -1;  // registry assigns
                            try {
                                id = topicRegistry_->createTopic(spec);
                                // Task 8b (SINGLE-NODE): make the runtime topic
                                // produce/consume-capable by creating its user
                                // PartitionReplica(s) NOW, on this same control
                                // thread that owns TopicRegistry / replicaRegistry_
                                // / replTransport_ / MetadataService (no new
                                // cross-thread mutation).  The authoritative spec
                                // (assigned id + all fields, dirs/logs already
                                // created by createTopic) is read back from the
                                // registry and replayed through the boot path.
                                if (id >= 0) {
                                    const rune::node::TopicSpec* authSpec =
                                        topicRegistry_->topic(id);
                                    if (authSpec) {
                                        (void)createRuntimeUserReplicas(*authSpec);
                                    }
                                }
                            } catch (const std::exception& ex) {
                                spdlog::error("BrokerNode: admin createTopic('{}') failed: {}",
                                              req.name, ex.what());
                                id = -1;
                            }
                        }
                        CreateTopicReplyMsg reply;
                        reply.ticket  = req.ticket;
                        reply.topicId = id;
                        // Reply ring is 1-deep in flight (one DDL at a time) → never full.
                        (void)encodeCreateTopicReply(agent->replyRing(), reply);
                    }, 8);

                // (3) Drain response frames (admin → control) → session egress.
                work += agent->egressRing().drain(
                    [this](int32_t /*tid*/,
                           aeron::concurrent::AtomicBuffer& b,
                           aeron::util::index_t off,
                           aeron::util::index_t len) {
                        const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
                        const auto n = static_cast<std::size_t>(len);
                        if (n < 8) return;
                        int64_t session;
                        std::memcpy(&session, p, 8);
                        if (gateway_)
                            (void)gateway_->sendControlFrame(session, p + 8, n - 8);
                    }, 16);

                return work;
            });
    }

    // Select the runtime/gateway idle strategy (RUNE_RUNTIME_IDLE).  BusySpin →
    // the consensus+gateway thread never parks → sub-100µs produce/ack round-trips
    // (pegs the core; use on a dedicated CPU).  Default Backoff (safe).
    if (config_.runtimeIdleStrategy == NodeConfig::WorkerIdleStrategy::BusySpin) {
        idleStrategy_ = std::make_unique<rune::consensus::BusySpinIdleStrategy>();
        spdlog::info("BrokerNode[{}]: runtime idle strategy = BusySpin (low latency; core pegged)",
                     config_.nodeId);
    } else {
        // Backoff with configurable maxPark (RUNE_IDLE_MAX_PARK_US, default 200µs):
        // caps per-hop replication-round-trip wake latency without pegging the core
        // (busyspin here starves consensus heartbeats → re-elections on shared cores).
        idleStrategy_ = makeBackoffIdle(config_.idleMaxParkUs);
    }

    // AgentRunner drives the NodeRuntimeAgent (consensus + gateway on ONE thread).
    agentRunner_ = std::make_unique<rune::consensus::AgentRunner>(
        *runtimeAgent_, *idleStrategy_, clock_);

    // -----------------------------------------------------------------------
    // CP-6: Wire the LogCleanerAgent on its own AgentRunner thread.
    //
    // Prerequisite: replicaRegistry_ must be built (done above in buildReplication).
    // The cleaner never accesses segments_ directly; it uses the sealed-segment
    // snapshot (atomic published by the append thread) — so starting it here
    // (before any appends begin) is safe.
    //
    // cycleBudgetBytes default (128 MiB) matches Kafka's default dedupe buffer.
    // -----------------------------------------------------------------------
    if (replicaRegistry_) {
        // CP-8: enable segment coalescing when config_.enableCompactionCoalescing is true
        // (default: true for Kafka parity).  The coalescingRollBytes ctor param is kept
        // as a fallback default (64 MiB); per-topic roll is read from each replica's
        // segmentRollBytes() at compaction time inside LogCleanerAgent::doWork.

        // CP-10 Fix 1: thread ioMaxBytesPerSecond from TopicSpec into the Throttler.
        // The LogCleanerAgent uses a single shared Throttler across all topics; we
        // derive the effective rate as the MINIMUM positive ioMaxBytesPerSecond across
        // all configured topics (so the strictest topic limit governs the cleaner).
        // If no topic sets a limit (all -1), the throttle remains unlimited (-1).
        int64_t cleanerIoMaxBytesPerSec = -1; // -1 = unlimited (Kafka default)
        for (const auto& ts : config_.topics) {
            if (ts.ioMaxBytesPerSecond > 0) {
                if (cleanerIoMaxBytesPerSec < 0 ||
                    ts.ioMaxBytesPerSecond < cleanerIoMaxBytesPerSec) {
                    cleanerIoMaxBytesPerSec = ts.ioMaxBytesPerSecond;
                }
            }
        }

        cleanerAgent_ = std::make_unique<LogCleanerAgent>(
            replicaRegistry_.get(),
            /*cycleBudgetBytes=*/   128LL * 1024 * 1024,
            /*dedupeBufferBytes=*/  -1,
            /*ioMaxBytesPerSec=*/   cleanerIoMaxBytesPerSec,
            /*useCoalescing=*/      config_.enableCompactionCoalescing,
            /*coalescingRollBytes=*/64LL * 1024 * 1024);
        // Task 10 round 4: the cleaner never reads the registry — it gets its
        // own replica view over a ring from the control thread, which frees a
        // removed replica only after the cleaner acked the generation dropping it.
        if (runtimeAgent_) cleanerAgent_->attachViewChannel(runtimeAgent_->enableCleanerView());
        cleanerRunner_ = std::make_unique<rune::consensus::AgentRunner>(
            *cleanerAgent_, cleanerIdleStrategy_, cleanerClock_);
        spdlog::info("BrokerNode[{}]: LogCleanerAgent wired ({} replicas, coalescing={}, ioThrottle={})",
                     nodeId, replicaRegistry_->replicaCount(),
                     config_.enableCompactionCoalescing ? "on" : "off",
                     cleanerIoMaxBytesPerSec < 0 ? "unlimited" : std::to_string(cleanerIoMaxBytesPerSec) + "B/s");
    }
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
// BrokerNode::buildAdminWorker — Task 6
//
// Mirrors the __transaction_state block (TX-3) at ~lines 975-1069:
//   1. Open + recover a Log under dataDir/"__topic_schemas".
//   2. Build a PartitionConfig with topicId=-4 (kSchemaTopicId) and
//      cleanupPolicy=Compact (same as __transaction_state / __cluster_metadata).
//   3. Build a PartitionReplica, register in replicaRegistry_ + replTransport_.
//   4. Rebuild SchemaRegistry from the compacted log (rebuildFromLog).
//   5. Construct AdminDeps with real effects wired:
//        createTopic     → TopicRegistry::createTopic(spec)
//        appendDurable   → appendKeyed to schemaReplica_ (majority-durable, minIsr=2)
//        pushValidators  → enqueue SchemaUpdate WorkerOp to each worker via workerPool_
//        respondDefine / respondGet → stored in BrokerNode response callbacks
//          (seam for Task 8 gateway routing; for Task 6: no-op / discarded)
//   6. Construct AdminWorkerAgent + AdminWorkerRunner (dedicated thread).
// ---------------------------------------------------------------------------
void BrokerNode::buildAdminWorker() {
    const int32_t nodeId = config_.nodeId;
    try {
        const fs::path schemaDir = fs::path{config_.dataDir} / "__topic_schemas";
        fs::create_directories(schemaDir);

        schemaLog_ = std::make_unique<rune::storage::Log>(schemaDir.string());
        schemaLog_->recover();

        const NodeId selfId = static_cast<NodeId>(config_.nodeId);

        std::vector<NodeId> allNodeIds;
        allNodeIds.reserve(config_.peers.size());
        for (const auto& peer : config_.peers) {
            allNodeIds.push_back(static_cast<NodeId>(peer.id));
        }

        rune::replication::PartitionConfig scfg;
        scfg.topicId              = kSchemaTopicId;  // -4: sentinel for __topic_schemas
        scfg.partitionId          = 0;
        scfg.selfId               = selfId;
        // Majority-durable (same as __transaction_state): schema records must
        // survive leader failover without data loss.
        scfg.minIsr               = std::min(2, static_cast<int>(allNodeIds.size()));
        scfg.replicaLagMaxMs      = config_.replicaLagMaxMs;
        scfg.replicaLagMaxOffset  = 1'000'000'000;  // TIME-based lag only
        scfg.fetchIntervalNs      = config_.fetchIntervalNs;
        scfg.isrCheckIntervalNs   = 10'000'000LL;
        scfg.maxFetchBytes        = 1 << 20;
        // Compacted: keep latest schema record per topicId (key = topicId bytes).
        scfg.cleanupPolicy        = rune::replication::CleanupPolicy::Compact;
        scfg.minCleanableDirtyRatio = 0.5;
        scfg.deleteRetentionMs      = 86'400'000LL;  // 1 day tombstone retention
        scfg.segmentRollBytes       = config_.offsetLogSegmentRollBytes;

        // RULING-2 A: the schema replica is SOLELY owned + driven by the admin
        // thread.  It is therefore NOT registered with replTransport_ and NOT
        // added to replicaRegistry_ — this keeps it out of BOTH the control
        // thread's allReplicas() doWork loop AND the bridge's role-flip loop
        // (which iterates allReplicas() and would call becomeLeader/Follower on
        // the admin-owned replica from the control thread — the RULING-2 race).
        //
        // A dedicated InMemoryTransport backs it so no cross-thread transport
        // poll (AeronPartitionTransport::poll → replica->applyReplicated on the
        // control thread) ever touches it.  Real multi-node follower fetch
        // routing on an admin-thread-owned transport is Docker-gated (Task 8b).
        nullTransports_.push_back(
            std::make_unique<rune::replication::InMemoryTransport>());
        rune::replication::PartitionTransport* schemaTransport =
            nullTransports_.back().get();

        schemaReplica_ = std::make_unique<rune::replication::PartitionReplica>(
            scfg,
            rune::replication::Role::Follower,
            /*epoch=*/0,
            allNodeIds,
            allNodeIds,
            schemaLog_.get(),
            schemaTransport);

        // schemaReplica_ stays owned HERE (never handed to replicaRegistry_).
        rune::replication::PartitionReplica* schemaReplicaRaw = schemaReplica_.get();

        // Build the replica-set vector for the AdminWorker's becomeLeader ISR seed.
        std::vector<int32_t> replicaSetIds;
        replicaSetIds.reserve(allNodeIds.size());
        for (auto id : allNodeIds) replicaSetIds.push_back(static_cast<int32_t>(id));

        // Build the in-memory schema registry.
        schemaRegistry_ = std::make_unique<SchemaRegistry>();

        // ── rebuildFromLog ───────────────────────────────────────────────────
        // Replay the compacted __topic_schemas log [0, HW) and reconstruct the
        // in-memory SchemaRegistry.  Mirrors ReplicatedTxnStateStore::rebuildFromLog.
        // Each record outer batch contains a single record with:
        //   key   = topicId bytes (4 bytes, LE int32, used by the compactor)
        //   value = TopicSchema::serialize() payload
        // ────────────────────────────────────────────────────────────────────
        if (schemaReplicaRaw) {
            const rune::replication::Offset hw = schemaReplicaRaw->highWatermark();
            if (hw > 0) {
                static constexpr int32_t kChunkBytes = 64 * 1024 * 1024;
                rune::replication::Offset fetchOffset = 0;
                while (fetchOffset < hw) {
                    std::vector<uint8_t> data = schemaReplicaRaw->read(fetchOffset, kChunkBytes);
                    if (data.empty()) break;

                    std::size_t pos = 0;
                    rune::replication::Offset lastBatchEnd = fetchOffset;

                    while (pos < data.size()) {
                        const uint8_t*    batchStart = data.data() + pos;
                        const std::size_t remaining  = data.size() - pos;

                        if (remaining < static_cast<std::size_t>(
                                rune::protocol::batch::kRecordBatchOverhead))
                            break;

                        rune::protocol::RecordBatchReader reader(batchStart, remaining);
                        if (!reader.ensureValid()) break;

                        const int32_t tl = reader.totalLength();
                        if (tl <= 0 || static_cast<std::size_t>(tl) > remaining) break;

                        // Each record: key=topicId(4 bytes LE), value=TopicSchema payload.
                        reader.forEach([this](std::string_view /*key*/,
                                              std::string_view val,
                                              int64_t /*offset*/,
                                              int64_t /*ts*/) {
                            auto ts = TopicSchema::deserialize(
                                std::span<const uint8_t>(
                                    reinterpret_cast<const uint8_t*>(val.data()),
                                    val.size()));
                            if (ts) {
                                schemaRegistry_->applyRecord(*ts);
                            }
                        });

                        const rune::replication::Offset batchNextOffset =
                            reader.baseOffset() + reader.lastOffsetDelta() + 1;
                        if (batchNextOffset > lastBatchEnd)
                            lastBatchEnd = batchNextOffset;

                        pos += static_cast<std::size_t>(tl);
                    }

                    if (lastBatchEnd <= fetchOffset) break;
                    fetchOffset = lastBatchEnd;
                }
                spdlog::info(
                    "BrokerNode[{}]: __topic_schemas rebuilt from log ({} entries, hw={})",
                    nodeId, schemaRegistry_->runtimeIdHighWater(), hw);
            }
        }

        (void)schemaReplicaRaw;  // replica handed to the AdminWorker below

        // ── Wire AdminDeps ───────────────────────────────────────────────────
        // RULING-2 B: createTopic is NOT called on the admin thread.  Instead the
        // admin thread posts a CreateTopicRequest on createReqRing_ (admin →
        // control); the control thread (NodeRuntimeAgent) owns TopicRegistry +
        // ReplicaRegistry, performs createTopic + registers a user PartitionReplica
        // (which makes the topic visible to the produce path via the SAME
        // ReplicaRegistry generation → WorkerPool::reconcile mechanism the broker
        // already uses), and posts the authoritative id back on replyRing_.
        AdminDeps deps;

        deps.requestCreateTopic = [this](int64_t ticket, int32_t partitionCount,
                                         const std::string& name) -> bool {
            if (!adminWorkerAgent_) return false;
            CreateTopicReqMsg m;
            m.ticket         = ticket;
            m.partitionCount = partitionCount;
            m.name           = name;
            return encodeCreateTopicReq(adminWorkerAgent_->createReqRing(), m);
        };

        // Task 7: pushValidators — enqueue a SchemaUpdate WorkerOp to EVERY
        // produce worker's ingress ring.
        //
        // Admin→worker hand-off: this lambda is called on the AdminWorker thread
        // after a DefineTopic DDL commit (validate=true).  It fans out a
        // SchemaUpdate op to each worker in the pool by calling
        // worker.tryEnqueueOp().  Each worker builds a local rune::codec::Validator
        // from the blob on ITS OWN thread (shared-nothing — single-writer per
        // validators_ map entry).
        //
        // Back-pressure: if a worker's ingress ring is full, we log a warning and
        // skip that worker for this cycle.  The validator will be retried when a
        // later DefineTopic DDL commit fires (rare admin path — not a problem in
        // practice, and a full ring means the worker is processing under load).
        // A TODO(T8) can add a retry queue if needed.
        //
        // No blocking: the lambda enqueues and returns.  The worker thread applies
        // the op the next time it drains its ingress ring.
        deps.pushValidators = [this](const RegistryEntry& entry, int32_t topicId) {
            if (!workerPool_) {
                spdlog::warn(
                    "BrokerNode: pushValidators — no WorkerPool; "
                    "validator for topicId={} not pushed", topicId);
                return;
            }
            const int32_t nWorkers = workerPool_->workerCount();
            for (int32_t i = 0; i < nWorkers; ++i) {
                WorkerOp op;
                op.type              = WorkerOp::Type::SchemaUpdate;
                op.topicId           = topicId;
                op.schemaUpdateBlob  = entry.descriptorBlob; // copy (admin-path, not hot)
                if (!workerPool_->worker(i).tryEnqueueOp(std::move(op))) {
                    spdlog::warn(
                        "BrokerNode: pushValidators — worker {} ingress ring full; "
                        "SchemaUpdate for topicId={} dropped (back-pressure)",
                        i, topicId);
                }
            }
            spdlog::info(
                "BrokerNode: pushValidators — SchemaUpdate for topicId={} "
                "enqueued to {} workers", topicId, nWorkers);
        };

        // respondDefine / respondGet (RULING-2 F): encode the protocol response
        // frame and push it on egressRing_ (admin → control) as
        // [sessionId:i64][encoded frame].  The control thread drains egressRing_
        // in NodeRuntimeAgent and sends each frame on the originating session's
        // egress via gateway_->sendControlFrame (single-writer — no cross-thread
        // egress publication access from the admin thread).
        deps.respondDefine = [this](int64_t session, int64_t corr, int32_t topicId,
                                    uint16_t version, uint16_t errorCode) {
            if (!adminWorkerAgent_) return;
            namespace pc = rune::protocol::client;
            pc::DefineTopicResponse resp;
            resp.sessionId     = session;
            resp.correlationId = corr;
            resp.topicId       = topicId;
            resp.version       = version;
            resp.errorCode     = errorCode;
            uint8_t frame[pc::MessageHeader::ENCODED_SIZE + pc::DefineTopicResponse::BODY_FIXED];
            const std::size_t n = resp.encode(frame);
            auto& ring = adminWorkerAgent_->egressRing();
            const auto idx = ring.tryClaim(kMsgEgressFrame, 8 + n);
            if (idx < 0) return;  // ring full — DDL response dropped (rare); client retries
            uint8_t* p = ring.base() + idx;
            std::memcpy(p, &session, 8);
            std::memcpy(p + 8, frame, n);
            ring.commit(idx);
        };
        deps.respondGet = [this](int64_t session, int64_t corr, int32_t topicId,
                                 uint16_t version, const std::vector<uint8_t>& blob,
                                 uint16_t errorCode) {
            if (!adminWorkerAgent_) return;
            namespace pc = rune::protocol::client;
            pc::TopicSchemaResponse resp;
            resp.sessionId      = session;
            resp.correlationId  = corr;
            resp.topicId        = topicId;
            resp.version        = version;
            resp.errorCode      = errorCode;
            resp.descriptorBlob = blob.empty() ? nullptr : blob.data();
            resp.descriptorLen  = static_cast<uint32_t>(blob.size());
            const std::size_t sz = resp.encodedSize();
            std::vector<uint8_t> frame(sz);
            const std::size_t n = resp.encode(frame.data());
            auto& ring = adminWorkerAgent_->egressRing();
            const auto idx = ring.tryClaim(kMsgEgressFrame, 8 + n);
            if (idx < 0) return;
            uint8_t* p = ring.base() + idx;
            std::memcpy(p, &session, 8);
            std::memcpy(p + 8, frame.data(), n);
            ring.commit(idx);
        };

        // Construct AdminWorkerAgent (owns AdminWorker + the __topic_schemas
        // replica by raw ptr; the replica stays owned by BrokerNode::schemaReplica_).
        adminWorkerAgent_ = std::make_unique<AdminWorkerAgent>(
            *schemaRegistry_, schemaReplica_.get(), config_.nodeId,
            std::move(replicaSetIds), std::move(deps));

        // Dedicated thread (separate from gateway and produce workers).
        adminWorkerRunner_ = std::make_unique<rune::consensus::AgentRunner>(
            *adminWorkerAgent_, adminWorkerIdleStrategy_, adminWorkerClock_);

        spdlog::info(
            "BrokerNode[{}]: __topic_schemas persistence + AdminWorker wired (dir={})",
            nodeId, schemaDir.string());

    } catch (const std::exception& ex) {
        spdlog::error(
            "BrokerNode[{}]: AdminWorker setup failed: {}; schema DDL will not work",
            nodeId, ex.what());
        adminWorkerRunner_.reset();
        adminWorkerAgent_.reset();
        schemaRegistry_.reset();
        schemaReplica_.reset();
        schemaLog_.reset();
    }
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

    // 1a. E2 inc2b: stop per-worker AgentRunner threads FIRST.
    //     Workers hold non-owning replica pointers; they must be quiesced before
    //     replicaRegistry_ is torn down.  Stop them before the cleaner and main
    //     agent so no in-flight worker op races with a replica being freed.
    if (workerPool_) {
        workerPool_->stopAndJoinRunners();
        spdlog::info("BrokerNode[{}]: worker AgentRunner threads stopped", config_.nodeId);
    }
    // Shard agents reference workers + their drivers/clients; the runners that
    // drove them are joined, so nothing pumps a shard driver any more.
    shardAgents_.clear();

    // 1b-admin. Task 6: stop AdminWorker thread BEFORE the main agent runner.
    //     The AdminWorker calls appendDurable which uses the schema replica.
    //     The main agent drives the replica's doWork, so the admin thread must
    //     be quiesced before the main agent stops.
    if (adminWorkerRunner_) {
        adminWorkerRunner_->stopAndJoin();
        spdlog::info("BrokerNode[{}]: AdminWorker thread stopped", config_.nodeId);
    }
    // NOTE (Task 8a): do NOT reset adminWorkerAgent_ here.  The gateway holds it
    // as adminSink_ and the NodeRuntimeAgent's admin control hook captures it;
    // both run on the MAIN agent runner, which is still live at this point.  We
    // reset it only AFTER agentRunner_->stopAndJoin() below (the main duty cycle
    // is quiesced then), preventing a use-after-free of adminSink_ / the hook.

    // 1b. Stop the LogCleanerAgent BEFORE the main agent runner.
    //     This ensures the cleaner thread stops enqueuing swaps before the
    //     duty cycle stops draining them (avoids a lost-swap scenario where
    //     a swap is enqueued after the last drain).
    if (cleanerRunner_) {
        cleanerRunner_->stopAndJoin();
    }
    cleanerAgent_.reset();

    // 1c. Stop the main agent runner (signal + join the duty-cycle thread).
    if (agentRunner_) {
        agentRunner_->stopAndJoin();
    }
    // P2: the control thread is joined — only now may the pool's control-thread
    // state (ring-ownership mode) change.
    if (workerPool_) workerPool_->disarmAfterControlJoined();

    // Task 8a: main duty cycle is now quiesced — safe to drop the admin agent.
    // Clear the gateway's admin sink first (defensive; gateway is torn down below).
    if (gateway_) gateway_->setAdminSink(nullptr);
    adminWorkerAgent_.reset();

    // 2. Release the runtime agent and gateway FIRST — in this order:
    //      a. runtimeAgent_ (holds refs to nodeAgent_/agent_ + gateway_ + replication)
    //      b. gateway_ (owns the client ingress Subscription + pending egress
    //                   ExclusivePublications — must deregister before aeron_.reset())
    //      c. replication objects (replBridge_ → replicaRegistry_ → replTransport_)
    //         replTransport_ owns Aeron Subscription + ExclusivePublications
    //    BR-3b teardown ordering: AgentRunner stop → release gateway/nodeAgent/ingress/sink
    //    → close aeron → stop driver.
    runtimeAgent_.reset();    // composite agent (holds refs to inner agent + gateway + replication)
    workerPool_.reset();      // E1: holds non-owning replica ptrs; drop before replicaRegistry_ frees them
    // TX-3: destroy txn coordination service before gateway (holds gateway* + txnState*).
    txnCoordService_.reset(); // TransactionCoordinationService (no Aeron resources, holds gateway*)
    // CG-6: destroy group coordination service before gateway (it holds a gateway* ref).
    groupCoordService_.reset(); // GroupCoordinationService (no Aeron resources, holds gateway* + offsets*)
    gateway_.reset();         // ClientGateway: deregisters Subscription + ExclusivePubs
    // MD1 Task 4: destroy metadata service before replicaRegistry_.
    metadataService_.reset(); // MetadataService (holds raw ptr to metadata replica in registry)
    metadataReplica_.reset(); // no-op: ownership transferred to replicaRegistry_ in buildGateway
    metadataLog_.reset();     // Log for __cluster_metadata (file flush on dtor)
    // Task 6: destroy schema registry before replicaRegistry_ (schema replica in registry).
    // adminWorkerAgent_ is already reset above (in the runner stop block).
    schemaRegistry_.reset();  // SchemaRegistry (no raw ptrs to replicas — safe to reset here)
    schemaReplica_.reset();   // no-op: ownership transferred to replicaRegistry_ in buildAdminWorker
    schemaLog_.reset();       // Log for __topic_schemas (file flush on dtor)
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
    memoryLogs_.clear();      // InMemoryLogs outlive replicas — clear after replicaRegistry_ on stop() path
    replTransport_.reset();   // AeronPartitionTransport: deregisters replication Subscription + ExclusivePubs
    // Task 5 teardown fix: perWorkerReplTransports_ holds per-worker inbound Subscriptions +
    // per-peer ExclusivePublications.  Must be cleared AFTER replicaRegistry_ (no replica
    // must dereference a freed transport) and BEFORE aeron_.reset() / stopEmbeddedDriver()
    // so the Subscription/Publication dtors deregister from the conductor while it is
    // still alive.  Without this clear(), those dtors run in ~BrokerNode which fires
    // after stop() has already torn down the Aeron client → use-after-close on restart.
    perWorkerReplTransports_.clear(); // WorkerReplicationTransports: deregisters per-worker Subscription + ExclusivePubs
    // Per-worker ingress/egress deregister from their client while it is alive
    // (legacy: the node client; shard: the worker's own client).
    workerIngresses_.clear();
    workerEgresses_.clear();
    // Shard teardown: clients before their drivers, each driver handed to THIS
    // thread (its pumping worker is joined) before it closes.  Before the node
    // driver, which owns the parent aeron dir.
    teardownShardRuntimes_();
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
    nodeDriver_ = std::make_unique<EmbeddedDriver>(
        EmbeddedDriverConfig::fromNode(config_, config_.effectiveAeronDir()),
        DriverMode::Threaded);
}

// ---------------------------------------------------------------------------
// BrokerNode::stopEmbeddedDriver
// ---------------------------------------------------------------------------
void BrokerNode::stopEmbeddedDriver() noexcept {
    nodeDriver_.reset();   // joins the driver thread, drains, closes (EmbeddedDriver::close)
}

// ---------------------------------------------------------------------------
// BrokerNode::buildShardRuntimes_ — spec §3.1: per worker, an invoker driver
// (<aeronDir>/w<w>) + an Aeron client with useConductorAgentInvoker(true).
// Refuses (throws) when the aeron dir cannot hold one log per driver.
// ---------------------------------------------------------------------------
void BrokerNode::buildShardRuntimes_() {
    const int32_t wc = config_.workerCount >= 1 ? config_.workerCount : 1;
    std::error_code ec;
    const auto space = fs::space(config_.effectiveAeronDir(), ec);
    if (ec) {
        spdlog::warn("BrokerNode[{}]: cannot stat free space of aeron dir {} ({}) — shard tmpfs budget "
                     "NOT checked", config_.nodeId, config_.effectiveAeronDir(), ec.message());
    } else if (auto err = checkAeronSpace(space.available, wc + 1, config_.aeronTermBufferBytes)) {
        // wc shard drivers + the node driver share the aeron filesystem.
        throw std::runtime_error("BrokerNode[" + std::to_string(config_.nodeId) +
                                 "]: refusing shard mode — " + *err);
    }
    shardDrivers_.reserve(static_cast<std::size_t>(wc));
    shardClients_.reserve(static_cast<std::size_t>(wc));
    for (int32_t w = 0; w < wc; ++w) {
        const std::string dir = (fs::path{config_.effectiveAeronDir()} / ("w" + std::to_string(w))).string();
        auto dcfg = EmbeddedDriverConfig::fromNode(config_, dir);
        dcfg.clientLivenessTimeoutNs = config_.shardClientLivenessTimeoutNs;
        shardDrivers_.push_back(std::make_unique<EmbeddedDriver>(std::move(dcfg), DriverMode::Invoker));
        aeron::Context ctx;
        ctx.aeronDir(dir);
        ctx.useConductorAgentInvoker(true);
        auto client = aeron::Aeron::connect(ctx);
        if (!client)
            throw std::runtime_error("BrokerNode[" + std::to_string(config_.nodeId) + "]: shard " +
                                     std::to_string(w) + " Aeron client connect failed");
        shardClients_.push_back(std::move(client));
    }
    spdlog::info("BrokerNode[{}]: {} shard runtime(s) (invoker driver + invoker client) under {}/w*",
                 config_.nodeId, wc, config_.effectiveAeronDir());
}

namespace {
// Startup-only composite pump: one pass of every shard's own AeronPump (driver
// duty cycle + client conductor).  A refusal from any shard (not its pump
// thread) fails the resolve fast, exactly like a single shard pump.
struct ShardStartupPump {
    std::vector<rune::aeronutil::AeronPump> pumps;
    static int pumpAll(void* self) noexcept {
        int work = 0;
        for (auto& p : static_cast<ShardStartupPump*>(self)->pumps) {
            const int w = p.pump();
            if (w < 0) return rune::aeronutil::AeronPump::kRefused;
            work += w;
        }
        return work;
    }
};
} // namespace

// ---------------------------------------------------------------------------
// BrokerNode::attachShardRuntimes_ — right after buildShardRuntimes_(), right
// before the runners start (no long phase in between).  Per worker w, on its OWN
// client, resolved by pumping its OWN invoker driver from this thread: the
// deferred replication plane (handshake enabled), client ingress, record egress,
// then its ShardAgent.  Any unresolved resource is a hard refusal (throw).
// ---------------------------------------------------------------------------
void BrokerNode::attachShardRuntimes_() {
    const int32_t wc = static_cast<int32_t>(shardClients_.size());
    if (!workerPool_ || workerPool_->workerCount() != wc)
        throw std::runtime_error("BrokerNode[" + std::to_string(config_.nodeId) +
                                 "]: shard runtime count does not match the worker pool — refusing to start");
    int32_t egressBase = config_.workerEgressBaseStreamId;
    if (egressBase <= 0) egressBase = config_.workerIngressBaseStreamId + wc;
    const int32_t egressControlBase = config_.effectiveWorkerEgressControlBasePort();
    shardAgents_.reserve(static_cast<std::size_t>(wc));
    // Every resolve below pumps EVERY shard's driver + client (each through its
    // own AeronPump; all are still owned by this start thread), so no shard goes
    // unpumped while another one resolves: startup liveness is independent of
    // the shard count and of how long any single resolve takes (fix round 2).
    ShardStartupPump startupPump;
    startupPump.pumps.reserve(static_cast<std::size_t>(wc));
    for (int32_t w = 0; w < wc; ++w)
        startupPump.pumps.push_back(shardDrivers_[static_cast<std::size_t>(w)]->pumpWith(
            shardClients_[static_cast<std::size_t>(w)].get()));
    for (int32_t w = 0; w < wc; ++w) {
        const auto idx = static_cast<std::size_t>(w);
        rune::aeronutil::AeronPump pump(&ShardStartupPump::pumpAll, &startupPump, nullptr);
        if (w == shardResolveStallWorkerForTest_) [[unlikely]] {
            // Test-only fault: a resolve of THIS shard that cannot complete for the
            // configured time (pumping exactly as a real slow resolve would).
            (void)pump.resolve([] { return false; }, shardResolveStallForTest_);
        }
        if (!perWorkerReplTransports_.empty()) {
            std::vector<std::pair<rune::replication::NodeId, std::string>> peersW;
            std::vector<rune::replication::NodeId> peerIds;
            for (const auto& peer : config_.peers) {
                if (peer.id == config_.nodeId) continue;
                peersW.push_back({static_cast<rune::replication::NodeId>(peer.id),
                                  NodeConfig::shardPeerReplChannel(peer, w)});
                peerIds.push_back(static_cast<rune::replication::NodeId>(peer.id));
            }
            auto& plane = *perWorkerReplTransports_[idx];
            if (!plane.attach(shardClients_[idx], peersW, config_.replBaseStreamId + w, pump))
                throw std::runtime_error("BrokerNode[" + std::to_string(config_.nodeId) + "]: shard " +
                                         std::to_string(w) + " replication plane did not resolve — refusing to start");
            // Runtime worker-count symmetry (Task 6 ruling I2): peers' RUNE_WORKERS
            // are unknown at parse time, so each plane verifies them in-band.
            plane.enablePlaneHandshake(static_cast<rune::replication::NodeId>(config_.nodeId), wc, peerIds);
        }
        auto ing = std::make_unique<WorkerIngress>(shardClients_[idx], config_.shardIngressBindChannel(w),
                                                   config_.workerIngressBaseStreamId + w, pump);
        const std::string control = config_.workerEgressControlHost + ":" + std::to_string(egressControlBase + w);
        auto eg = std::make_unique<WorkerEgress>(shardClients_[idx], control, egressBase + w, pump);
        if (!ing->resolved() || !eg->resolved())
            throw std::runtime_error("BrokerNode[" + std::to_string(config_.nodeId) + "]: shard " +
                                     std::to_string(w) + " ingress/egress did not resolve — refusing to start");
        Worker& wk = workerPool_->worker(w);
        wk.setWorkerIngress(ing.get(), /*active=*/true);
        wk.setWorkerEgress(eg.get());
        workerIngresses_.push_back(std::move(ing));
        workerEgresses_.push_back(std::move(eg));
        shardAgents_.push_back(std::make_unique<ShardAgent>(wk, *shardDrivers_[idx], *shardClients_[idx], clock_));
    }
    if (gateway_) {
        gateway_->setPerWorkerIngressActive(true);
        gateway_->setPerWorkerEgressActive(true);
        gateway_->setWorkerEgressParams(egressControlBase, egressBase);
        if (topicRegistry_)
            for (const auto& ts : config_.topics)
                for (int32_t p = 0; p < ts.partitionCount; ++p)
                    if (auto* log = topicRegistry_->log(ts.topicId, p)) gateway_->addWorkerOwnedLog(log);
    }
    spdlog::info("BrokerNode[{}]: {} worker shard(s) attached (own driver + client + plane + ingress + egress)",
                 config_.nodeId, wc);
}

// ---------------------------------------------------------------------------
// BrokerNode::teardownShardRuntimes_ — precondition: every shard runner joined.
// Hand-off (EmbeddedDriver one-pumper contract): the closing thread binds itself
// as each invoker driver's pumper, clients close (their close commands queue in
// the CnC ring), then each driver drains + closes on this thread.  A driver that
// still refuses to close is a leak: logged critical, debug builds abort.
// ---------------------------------------------------------------------------
void BrokerNode::teardownShardRuntimes_() noexcept {
    if (shardDrivers_.empty() && shardClients_.empty()) return;
    shardAgents_.clear();
    for (auto& d : shardDrivers_) if (d) d->bindPumpThread();
    // Clients first (their close commands queue in each driver's CnC ring), then
    // drivers.  ~Aeron and EmbeddedDriver::close() are both noexcept and Aeron
    // exposes no throwing close() to wrap, so there is nothing to catch here: a
    // failing close terminates per the C++ contract.  What CAN go wrong without
    // throwing — a driver close refused (wrong pump thread) — is checked below.
    shardClients_.clear();
    for (auto& d : shardDrivers_) {
        if (!d) continue;
        d->close();
        const bool open = d->isOpen();
        if (open) {
            spdlog::critical("BrokerNode[{}]: shard driver {} did not close on the stopping thread — LEAK",
                             config_.nodeId, d->dir());
#ifndef NDEBUG
            std::abort();
#endif
            (void)d.release();   // never re-run a failing close from the destructor
        }
    }
    shardDrivers_.clear();
}

} // namespace rune::node
