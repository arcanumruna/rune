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

    // 2. Start embedded Aeron media driver.
    startEmbeddedDriver();
    const auto t1 = sclock::now();

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
    const auto t2 = sclock::now();

    // 4. Build all consensus objects (NodeStateFile, TermLog, Election, …).
    buildConsensusObjects();
    const auto t3 = sclock::now();

    // 5. Start the agent runner thread.
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

    // AgentRunner drives the ConsensusAgent directly.
    agentRunner_ = std::make_unique<AgentRunner>(*agent_, idleStrategy_, clock_);
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

    // 2. Release consensus objects that own Aeron publications / subscription.
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

    // 3. Close the Aeron client (joins the C++ conductor thread).
    //    After aeron_.reset() all "remove publication / subscription / close client"
    //    commands have been written into the CnC ring by the conductor's onClose().
    if (aeron_) {
        aeron_.reset();
    }

    // 4. Stop the embedded media driver.
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
