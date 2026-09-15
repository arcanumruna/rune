// ---------------------------------------------------------------------------
// AeronPartitionTransportIntegrationTest.cpp — GR-1 in-process Aeron
// integration test for AeronPartitionTransport over aeron:udp on loopback.
//
// NOT Docker.  Uses the same EmbeddedMediaDriver pattern as
// tests/integration/AeronTransportLoopbackTest.cpp (Phase-2 / BR-3).
//
// Tests:
//   1. AeronReplFetchLoop_TwoNodes:
//      - One EmbeddedMediaDriver (SHARED threading mode).
//      - One Aeron client (shared, thread-safe ClientConductor).
//      - Leader replica: ingress on port A, egress pub → follower on port B.
//      - Follower replica: ingress on port B, egress pub → leader on port A.
//      - Both AeronPartitionTransports register each other as peers.
//      - Leader appends 5 records.
//      - Duty-cycle thread drives both transports' poll() + replicas' doWork()
//        for up to 10 s (hard timeout → FAIL, not hang).
//      - Assert: follower logEndOffset() >= leader logEndOffset(), and
//        both highWatermark() converge to the leader's LEO.
//
// Design constraints (Aeron-bible / DR-BUG-2):
//   - poll() is NON-BLOCKING — no sleep inside the transport.
//   - A single background thread drives both duty cycles so we can run
//     the assertion on the main thread with a real-time deadline.
//   - Bounded: hard deadline 10 s; FAIL if not converged.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <Aeron.h>
#include <Context.h>

#include <rune/replication/AeronPartitionTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>

// UDP port discovery
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

using namespace rune::replication;
using namespace std::chrono;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// EmbeddedMediaDriver — thin RAII wrapper (mirrors AeronTransportLoopbackTest)
// ---------------------------------------------------------------------------
class EmbeddedMediaDriver {
public:
    explicit EmbeddedMediaDriver(const std::string& dir = "") {
        if (aeron_driver_context_init(&ctx_) < 0) {
            throw std::runtime_error(std::string("driver context init: ") + aeron_errmsg());
        }
        if (!dir.empty()) {
            aeron_driver_context_set_dir(ctx_, dir.c_str());
        }
        aeron_driver_context_set_threading_mode(ctx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(ctx_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(ctx_, true);
        aeron_driver_context_set_shared_idle_strategy(ctx_, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(ctx_, true);
        aeron_driver_context_set_term_buffer_length(ctx_, 64 * 1024);
        aeron_driver_context_set_ipc_term_buffer_length(ctx_, 64 * 1024);

        if (aeron_driver_init(&driver_, ctx_) < 0) {
            aeron_driver_context_close(ctx_);
            throw std::runtime_error(std::string("driver init: ") + aeron_errmsg());
        }
        if (aeron_driver_start(driver_, true) < 0) {
            aeron_driver_close(driver_);
            aeron_driver_context_close(ctx_);
            throw std::runtime_error(std::string("driver start: ") + aeron_errmsg());
        }
        aeronDir_ = aeron_driver_context_get_dir(ctx_);
        running_.store(true, std::memory_order_release);
        thread_ = std::thread([this]() {
            while (running_.load(std::memory_order_acquire)) {
                aeron_driver_main_idle_strategy(driver_, aeron_driver_main_do_work(driver_));
            }
        });
    }

    ~EmbeddedMediaDriver() {
        running_.store(false, std::memory_order_release);
        if (thread_.joinable()) thread_.join();
        aeron_driver_close(driver_);
        aeron_driver_context_close(ctx_);
    }

    [[nodiscard]] const std::string& aeronDir() const { return aeronDir_; }

private:
    aeron_driver_context_t* ctx_    = nullptr;
    aeron_driver_t*         driver_ = nullptr;
    std::string             aeronDir_;
    std::atomic<bool>       running_{false};
    std::thread             thread_;
};

// ---------------------------------------------------------------------------
// Unique temp dir helper
// ---------------------------------------------------------------------------
struct TempDir {
    fs::path dir;
    explicit TempDir(const std::string& prefix = "rune_repl_integ_") {
        static std::atomic<int> ctr{0};
        auto ts = steady_clock::now().time_since_epoch().count();
        dir = fs::temp_directory_path() / (prefix + std::to_string(::getpid()) + "_" +
                                           std::to_string(ts) + "_" +
                                           std::to_string(ctr.fetch_add(1)));
        fs::create_directories(dir);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(dir, ec); }
};

// ---------------------------------------------------------------------------
// Port discovery
// ---------------------------------------------------------------------------
static uint16_t discoverFreeUdpPort() {
    int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) throw std::runtime_error("socket() failed");

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = 0;
    addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");
    if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        throw std::runtime_error("bind() failed");
    }
    socklen_t addrLen = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &addrLen);
    const uint16_t netPort = addr.sin_port;
    const uint16_t port =
        static_cast<uint16_t>((netPort >> 8) | ((netPort & 0xFFu) << 8));
    ::close(fd);
    return port;
}

static std::string udpChannel(uint16_t port) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(port);
}

// ---------------------------------------------------------------------------
// PartitionConfig helper
// ---------------------------------------------------------------------------
static PartitionConfig makeConfig(NodeId selfId, PartitionId pid = 0) {
    PartitionConfig cfg;
    cfg.partitionId         = pid;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 5000;
    cfg.replicaLagMaxOffset = 100000;
    cfg.minIsr              = 1;
    cfg.fetchIntervalNs     = 5'000'000LL;    // 5 ms — default
    cfg.isrCheckIntervalNs  = 10'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    return cfg;
}

// ===========================================================================
// TEST: two-node leader/follower fetch loop over real aeron:udp
// ===========================================================================
TEST(AeronPartitionTransportInteg, FetchLoop_TwoNodes) {
    // -----------------------------------------------------------------------
    // 0.  Temp dirs + embedded media driver
    // -----------------------------------------------------------------------
    TempDir driverTd("rune_repl_driver_");
    std::string driverDir = (driverTd.dir / "aeron").string();

    // Discover free UDP ports for the two ingress endpoints.
    const uint16_t portLeader   = discoverFreeUdpPort();
    const uint16_t portFollower = discoverFreeUdpPort();

    ASSERT_NE(portLeader, portFollower) << "Port collision — retry";

    const std::string chanLeader   = udpChannel(portLeader);
    const std::string chanFollower = udpChannel(portFollower);
    constexpr int32_t kReplStream  = 500; // arbitrary stream id for replication

    EmbeddedMediaDriver driver(driverDir);

    // -----------------------------------------------------------------------
    // 1.  Shared Aeron client
    // -----------------------------------------------------------------------
    aeron::Context aeronCtx;
    aeronCtx.aeronDir(driver.aeronDir());
    aeronCtx.errorHandler([](const std::exception& e) {
        fprintf(stderr, "[AeronPartitionTransportInteg] Aeron error: %s\n", e.what());
    });
    auto aeronClient = aeron::Aeron::connect(aeronCtx);

    // -----------------------------------------------------------------------
    // 2.  Log storage (in-process temp dirs)
    // -----------------------------------------------------------------------
    TempDir logDirLeader("ldr");
    TempDir logDirFollower("flw");
    rune::storage::Log ldrLog(logDirLeader.dir.string());
    rune::storage::Log flwLog(logDirFollower.dir.string());

    // -----------------------------------------------------------------------
    // 3.  AeronPartitionTransports
    //
    //   Leader ingress = chanLeader / kReplStream
    //   Leader egress  = chanFollower / kReplStream  (to reach follower)
    //
    //   Follower ingress = chanFollower / kReplStream
    //   Follower egress  = chanLeader   / kReplStream  (to reach leader)
    // -----------------------------------------------------------------------
    constexpr NodeId kLeaderId   = 1;
    constexpr NodeId kFollowerId = 2;
    constexpr PartitionId kPid   = 0;

    // Leader transport: listens on chanLeader; sends to chanFollower.
    auto ldrTransport = std::make_unique<AeronPartitionTransport>(
        aeronClient, chanLeader, kReplStream, kLeaderId);
    ldrTransport->addPeer(kFollowerId, chanFollower, kReplStream);

    // Follower transport: listens on chanFollower; sends to chanLeader.
    auto flwTransport = std::make_unique<AeronPartitionTransport>(
        aeronClient, chanFollower, kReplStream, kFollowerId);
    flwTransport->addPeer(kLeaderId, chanLeader, kReplStream);

    // -----------------------------------------------------------------------
    // 4.  PartitionReplicas
    // -----------------------------------------------------------------------
    std::vector<NodeId> nodes = {kLeaderId, kFollowerId};
    std::vector<NodeId> isr   = {kLeaderId, kFollowerId};

    PartitionReplica leaderReplica(makeConfig(kLeaderId, kPid),
                                   Role::Leader, 1, nodes, isr,
                                   &ldrLog, ldrTransport.get());
    PartitionReplica followerReplica(makeConfig(kFollowerId, kPid),
                                     Role::Follower, 1, nodes, isr,
                                     &flwLog, flwTransport.get());

    // Wire routing: each transport → its local replica.
    // DR-BUG-5: pass topicId (0 here) as the first argument.
    ldrTransport->registerReplica(0, kPid, &leaderReplica);
    flwTransport->registerReplica(0, kPid, &followerReplica);

    // -----------------------------------------------------------------------
    // 5.  Leader: append 5 batches before the drive loop starts.
    // -----------------------------------------------------------------------
    constexpr int kNumBatches = 5;
    Offset targetLeo = -1;
    for (int i = 0; i < kNumBatches; ++i) {
        std::vector<uint8_t> payload(32, static_cast<uint8_t>(0xA0 + i));
        Offset off = leaderReplica.append(payload);
        ASSERT_GE(off, 0) << "Leader append failed at batch " << i;
        targetLeo = leaderReplica.logEndOffset();
    }
    ASSERT_GT(targetLeo, 0);

    // -----------------------------------------------------------------------
    // 6.  Duty-cycle: drive both transports + replicas until convergence.
    //     Hard real-time deadline: 10 s.  Not a spin — yields via sleep_for.
    // -----------------------------------------------------------------------
    using Wall = std::chrono::steady_clock;
    const auto deadline = Wall::now() + seconds{10};

    bool converged = false;
    int64_t simNs = 0;
    constexpr int64_t kStepNs = 5'000'000LL;   // 5 ms simulated clock step

    while (Wall::now() < deadline) {
        simNs += kStepNs;

        // Transport polls (drain ingress, resolve pending pubs).
        ldrTransport->poll(simNs);
        flwTransport->poll(simNs);

        // Replica duty-cycle work (follower emits FetchRequests; leader runs ISR).
        leaderReplica.doWork(simNs);
        followerReplica.doWork(simNs);

        if (followerReplica.logEndOffset() >= targetLeo &&
            leaderReplica.highWatermark() >= targetLeo) {
            converged = true;
            break;
        }

        // Brief yield — not a busy-spin; gives the embedded driver time to
        // process frames.  The transport itself is fully non-blocking.
        std::this_thread::sleep_for(milliseconds{1});
    }

    // -----------------------------------------------------------------------
    // 7.  Assertions
    // -----------------------------------------------------------------------
    ASSERT_TRUE(converged)
        << "Replication did not converge within 10 s. "
        << "Follower LEO=" << followerReplica.logEndOffset()
        << " targetLeo=" << targetLeo
        << " leader HW=" << leaderReplica.highWatermark()
        << " unresolvedPeerCount (ldr)=" << ldrTransport->unresolvedPeerCount()
        << " unresolvedPeerCount (flw)=" << flwTransport->unresolvedPeerCount();

    EXPECT_GE(followerReplica.logEndOffset(), targetLeo)
        << "Follower LEO did not reach leader LEO";
    EXPECT_GE(leaderReplica.highWatermark(), targetLeo)
        << "Leader HW did not advance to LEO (follower must have acknowledged)";
}

// ===========================================================================
// TEST: large-backlog convergence — GR-1 regression test
//
// Leader pre-loads ≥500 batches (each 100-byte payload → ~173 bytes on-disk
// after RecordBatch framing), totalling ~86,500 bytes — well over the
// production 64 KiB term's maxMessageLength of 8192 bytes.
//
// Pre-fix: AeronPartitionTransport::sendOnPub called offer() with a
//   FetchResponse of up to 1 MiB → offer() throws IllegalArgumentException →
//   duty-cycle thread crash → follower LEO stays 0 forever (livelock).
//
// Post-fix: req.maxBytes is capped to maxMessageLength - 42 = 8150 bytes
//   before onFetchRequest reads the log, and whole-batch alignment ensures the
//   encoded response never exceeds 8150 bytes.  The follower drains the
//   ~86,500-byte backlog across ~11 fetches and fully converges.
//
// This test uses the PRODUCTION 64 KiB term (same as BrokerNode) to prove
// the fix works at the real production ceiling, not a padded configuration.
// ===========================================================================
TEST(AeronPartitionTransportInteg, FetchLoop_LargeBacklog) {
    // -----------------------------------------------------------------------
    // 0.  Temp dirs + embedded media driver (production 64 KiB term)
    // -----------------------------------------------------------------------
    TempDir driverTd("rune_repl_large_driver_");
    std::string driverDir = (driverTd.dir / "aeron").string();

    const uint16_t portLeader   = discoverFreeUdpPort();
    const uint16_t portFollower = discoverFreeUdpPort();
    ASSERT_NE(portLeader, portFollower) << "Port collision — retry";

    const std::string chanLeader   = udpChannel(portLeader);
    const std::string chanFollower = udpChannel(portFollower);
    constexpr int32_t kReplStream  = 501;

    // Production-identical: 64 KiB term → maxMessageLength = 8192 bytes.
    // The fix must drain 500 batches across multiple fetches without ever
    // attempting a send > 8192 bytes.
    EmbeddedMediaDriver driver(driverDir);

    // -----------------------------------------------------------------------
    // 1.  Shared Aeron client
    // -----------------------------------------------------------------------
    aeron::Context aeronCtx;
    aeronCtx.aeronDir(driver.aeronDir());
    aeronCtx.errorHandler([](const std::exception& e) {
        fprintf(stderr, "[AeronPartitionTransportInteg/LargeBacklog] Aeron error: %s\n",
                e.what());
    });
    auto aeronClient = aeron::Aeron::connect(aeronCtx);

    // -----------------------------------------------------------------------
    // 2.  Log storage
    // -----------------------------------------------------------------------
    TempDir logDirLeader("ldr_large");
    TempDir logDirFollower("flw_large");
    rune::storage::Log ldrLog(logDirLeader.dir.string());
    rune::storage::Log flwLog(logDirFollower.dir.string());

    // -----------------------------------------------------------------------
    // 3.  AeronPartitionTransports
    // -----------------------------------------------------------------------
    constexpr NodeId kLeaderId   = 3;
    constexpr NodeId kFollowerId = 4;
    constexpr PartitionId kPid   = 1;

    auto ldrTransport = std::make_unique<AeronPartitionTransport>(
        aeronClient, chanLeader, kReplStream, kLeaderId);
    ldrTransport->addPeer(kFollowerId, chanFollower, kReplStream);

    auto flwTransport = std::make_unique<AeronPartitionTransport>(
        aeronClient, chanFollower, kReplStream, kFollowerId);
    flwTransport->addPeer(kLeaderId, chanLeader, kReplStream);

    // -----------------------------------------------------------------------
    // 4.  PartitionReplicas
    // -----------------------------------------------------------------------
    std::vector<NodeId> nodes = {kLeaderId, kFollowerId};
    std::vector<NodeId> isr   = {kLeaderId, kFollowerId};

    PartitionReplica leaderReplica(makeConfig(kLeaderId, kPid),
                                   Role::Leader, 1, nodes, isr,
                                   &ldrLog, ldrTransport.get());
    PartitionReplica followerReplica(makeConfig(kFollowerId, kPid),
                                     Role::Follower, 1, nodes, isr,
                                     &flwLog, flwTransport.get());

    // DR-BUG-5: pass topicId (0 here) as the first argument.
    ldrTransport->registerReplica(0, kPid, &leaderReplica);
    flwTransport->registerReplica(0, kPid, &followerReplica);

    // -----------------------------------------------------------------------
    // 5.  Leader: append 500 batches BEFORE the drive loop.
    //
    //   Each payload = 100 bytes.
    //   On-disk size per batch ≈ 61 (header) + ~12 (varint overhead) + 100 = ~173 bytes.
    //   Total backlog ≈ 500 × 173 = ~86,500 bytes.
    //
    //   With production 64 KiB term (maxMessageLength = 8192):
    //     ceiling per fetch = 8192 - 42 (FetchResponse overhead) = 8150 bytes
    //     batches per fetch ≈ 8150 / 173 ≈ 47 batches/fetch
    //     fetches needed to drain ≈ 500 / 47 ≈ 11 fetches (multi-fetch drain).
    //
    //   Pre-fix: offer() throws on first fetch (437 KB response) → livelock.
    //   Post-fix: drains in ~11 fetches → convergence.
    // -----------------------------------------------------------------------
    constexpr int kNumBatches = 500;
    Offset targetLeo = -1;
    for (int i = 0; i < kNumBatches; ++i) {
        std::vector<uint8_t> payload(100, static_cast<uint8_t>(i & 0xFF));
        Offset off = leaderReplica.append(payload);
        ASSERT_GE(off, 0) << "Leader append failed at batch " << i;
        targetLeo = leaderReplica.logEndOffset();
    }
    ASSERT_GT(targetLeo, 0);
    ASSERT_GE(targetLeo, static_cast<Offset>(kNumBatches))
        << "Leader LEO must be ≥ " << kNumBatches << " (one offset per batch)";

    // -----------------------------------------------------------------------
    // 6.  Duty-cycle: drive until convergence (hard 30 s wall-clock deadline).
    //
    //   With 5 ms fetch interval and ~11 fetches needed:
    //     minimum wall-clock time ≈ 11 × 5 ms = 55 ms.
    //   Budget of 30 s is generous (allows for CI slow-down).
    // -----------------------------------------------------------------------
    using Wall = std::chrono::steady_clock;
    const auto deadline = Wall::now() + seconds{30};

    bool converged = false;
    int64_t simNs = 0;
    constexpr int64_t kStepNs = 5'000'000LL; // 5 ms simulated clock step

    while (Wall::now() < deadline) {
        simNs += kStepNs;

        ldrTransport->poll(simNs);
        flwTransport->poll(simNs);

        leaderReplica.doWork(simNs);
        followerReplica.doWork(simNs);

        if (followerReplica.logEndOffset() >= targetLeo &&
            leaderReplica.highWatermark() >= targetLeo) {
            converged = true;
            break;
        }

        std::this_thread::sleep_for(milliseconds{1});
    }

    // -----------------------------------------------------------------------
    // 7.  Assertions
    // -----------------------------------------------------------------------
    ASSERT_TRUE(converged)
        << "Large-backlog replication did not converge within 30 s.\n"
        << "  Follower LEO=" << followerReplica.logEndOffset()
        << "  targetLeo="   << targetLeo
        << "  leaderHW="    << leaderReplica.highWatermark()
        << "\n  unresolvedPeerCount(ldr)=" << ldrTransport->unresolvedPeerCount()
        << "  unresolvedPeerCount(flw)="   << flwTransport->unresolvedPeerCount()
        << "\n  Pre-fix: offer() would throw on any response > 8192 bytes and"
           " the follower LEO would stay 0 forever.";

    EXPECT_GE(followerReplica.logEndOffset(), targetLeo)
        << "Follower LEO did not reach leader LEO";
    EXPECT_GE(leaderReplica.highWatermark(), targetLeo)
        << "Leader HW did not advance to LEO";
}
