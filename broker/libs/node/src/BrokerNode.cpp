#include <rune/node/BrokerNode.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>

namespace fs = std::filesystem;
using namespace rune::consensus;

namespace rune::node {

// ---------------------------------------------------------------------------
// Timing constants — mirror ConsensusAgentTest (§6) exactly so a 1-node
// cluster self-elects within ms, not seconds.
// ---------------------------------------------------------------------------
static constexpr int64_t kElectionTimeoutNs        = 150'000'000LL; // 150 ms
static constexpr int64_t kStatusIntervalNs         =  50'000'000LL; //  50 ms
static constexpr int64_t kStartupCanvassTimeoutNs  = 300'000'000LL; // 300 ms
static constexpr int      kIngressCapacity          = 64 * 1024;    //  64 KiB

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

    // 1. Create dataDir if missing.
    fs::path dataDir{config_.dataDir};
    std::error_code ec;
    fs::create_directories(dataDir, ec);
    if (ec) {
        throw std::runtime_error("BrokerNode: cannot create dataDir '" +
                                 config_.dataDir + "': " + ec.message());
    }

    // 2. Start embedded Aeron media driver.
    startEmbeddedDriver();

    // 3. Connect Aeron client — must point at the same dir as the driver.
    {
        const std::string aeronDir =
            (fs::path{config_.dataDir} / "aeron").string();
        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron_ = aeron::Aeron::connect(ctx);
        if (!aeron_) {
            throw std::runtime_error("BrokerNode: failed to connect Aeron client");
        }
    }

    // 4. Build all consensus objects (NodeStateFile, TermLog, Election, …).
    buildConsensusObjects();

    // 5. Start the agent runner thread.
    agentRunner_->start();

    spdlog::info("BrokerNode: node {} started, dataDir={}", config_.nodeId, config_.dataDir);
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

    // Election — startup values matching the single-member test fixture.
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
        kElectionTimeoutNs,
        kStatusIntervalNs,
        kStartupCanvassTimeoutNs,
        /*rngSeed=*/42u);

    // ConsensusAgent (plain — no ingress polling needed for 1-node).
    agent_ = std::make_unique<ConsensusAgent>(*election_, *ingressRing_);

    // AgentRunner drives the ConsensusAgent directly.
    agentRunner_ = std::make_unique<AgentRunner>(*agent_, idleStrategy_, clock_);
}

// ---------------------------------------------------------------------------
// BrokerNode::buildMultiNodeConsensus — BR-3 path (UDP per-peer + ingress)
// ---------------------------------------------------------------------------
void BrokerNode::buildMultiNodeConsensus() {
    const NodeId selfId = static_cast<NodeId>(config_.nodeId);

    // Build per-peer UDP sink (publish to each OTHER peer's consensus endpoint).
    std::unordered_map<NodeId, UdpMessageSink::PeerEndpoint> peerEndpoints;
    for (const auto& peer : config_.peers) {
        if (peer.id == config_.nodeId) continue; // skip self
        peerEndpoints[static_cast<NodeId>(peer.id)] =
            UdpMessageSink::PeerEndpoint{
                peer.consensusChannel(),
                config_.consensusStreamId
            };
    }
    udpSink_ = std::make_unique<UdpMessageSink>(aeron_, peerEndpoints);

    // Publisher over the UDP sink.
    publisher_ = std::make_unique<SbeConsensusPublisher>(*udpSink_);

    // Election (multi-member).
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
        kElectionTimeoutNs,
        kStatusIntervalNs,
        kStartupCanvassTimeoutNs,
        /*rngSeed=*/static_cast<uint64_t>(42u + selfId));

    // AeronIngress — subscribe on THIS node's own consensus endpoint.
    const auto& selfPeer = config_.self();
    aeronIngress_ = std::make_unique<AeronIngress>(
        aeron_,
        selfPeer.consensusChannel(),
        config_.consensusStreamId,
        *ingressRing_);

    // Inner ConsensusAgent (drives Election, drains IngressRing).
    agent_ = std::make_unique<ConsensusAgent>(*election_, *ingressRing_);

    // Composite NodeConsensusAgent (polls ingress THEN runs ConsensusAgent).
    nodeAgent_ = std::make_unique<NodeConsensusAgent>(
        *aeronIngress_, *agent_, *election_, selfId);

    // AgentRunner drives the composite agent.
    agentRunner_ = std::make_unique<AgentRunner>(*nodeAgent_, idleStrategy_, clock_);
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
    // Use the atomic stateCode() — release-written by the agent thread on every
    // state transition — instead of the non-atomic state() accessor, to avoid a
    // data race when this method is called from the node/main thread.
    // Leader states (Aeron ordinals): LeaderLogReplication=5, LeaderReady=8, Closed=17.
    const int32_t c = election_->stateCode();
    const bool leaderState =
        c == static_cast<int32_t>(ElectionState::LeaderLogReplication) ||
        c == static_cast<int32_t>(ElectionState::LeaderReady)          ||
        c == static_cast<int32_t>(ElectionState::Closed);

    if (!leaderState) return false;

    // In the multi-node case a node may transition through Closed as a FOLLOWER.
    // Distinguish by checking the observed leader id: if it equals self, we are leader.
    if (nodeAgent_) {
        return nodeAgent_->leaderId() == static_cast<NodeId>(config_.nodeId);
    }
    // Single-node: Closed always means self-elected.
    return true;
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
// BrokerNode::stop
// ---------------------------------------------------------------------------
void BrokerNode::stop() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true,
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return; // idempotent
    }

    // Stop the agent runner (signal + join).
    if (agentRunner_) {
        agentRunner_->stopAndJoin();
    }

    // Close the Aeron client.
    if (aeron_) {
        aeron_.reset();
    }

    // Stop the embedded driver.
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

    // Give the driver a moment to fully initialise before we connect the client.
    // Known startup-sync approximation: the Aeron C API does not expose a
    // conductor-ready poll equivalent, so we use a fixed sleep.  50 ms is
    // empirically sufficient on CI; a proper poll would need aeron_driver_main_do_work
    // to return a "ready" sentinel, which is not part of the public C API.
    std::this_thread::sleep_for(std::chrono::milliseconds{50});

    spdlog::info("BrokerNode: embedded Aeron driver started (dir={})",
                 aeron_driver_context_get_dir(driverCtx_));
}

// ---------------------------------------------------------------------------
// BrokerNode::stopEmbeddedDriver
// ---------------------------------------------------------------------------
void BrokerNode::stopEmbeddedDriver() noexcept {
    // Signal the driver loop to stop.
    if (driverRunning_.exchange(false, std::memory_order_acq_rel)) {
        if (driverThread_.joinable()) {
            driverThread_.join();
        }
    }
    if (driver_) {
        aeron_driver_close(driver_);
        driver_ = nullptr;
    }
    if (driverCtx_) {
        aeron_driver_context_close(driverCtx_);
        driverCtx_ = nullptr;
    }
}

} // namespace rune::node
